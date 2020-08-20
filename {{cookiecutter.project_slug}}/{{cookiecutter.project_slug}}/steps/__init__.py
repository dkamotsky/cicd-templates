"""
steps module contains example definitions of Train and Evaluate pipeline steps based on Keras
"""


from typing import Optional, Iterable, Tuple
from pyspark import sql
from abc import ABC, abstractmethod
from mlflow.tracking import MlflowClient
from mlflow.exceptions import RestException
from mlflow.entities.model_registry import ModelVersion


class AbstractEvaluateAndPromoteStep(ABC):
    """
    Abstract pipeline step definition for evaluating and promoting an ML model.
    """

    def __init__(self, experiment_data: sql.DataFrame, model_name: str, **kwargs) -> None:
        """
            Creates an evaluate step definition.

            :param experiment_data: Spark dataframe with the MLflow experiment metadata
            :param model_name: str name for registering the model in MLflow Registry
            :return: returns nothing
        """
        self.model_name: str = model_name
        self.experiment_data: sql.DataFrame = experiment_data

    @abstractmethod
    def _evaluate_model(self, run_id: str, spark: sql.SparkSession) -> float:
        """
            Evaluates an ML model on a test data set.

            :param run_id: str MLflow run ID
            :param spark: Spark session managing the test data set
            :return: returns float evaluation score >= 0.0, bigger is better
        """
        raise NotImplementedError

    def __get_best_model(self, run_ids: Iterable[str], spark: sql.SparkSession) -> Tuple[float, Optional[str]]:
        """
            Retrieves the MLflow run with the best evaluation score.

            :param run_ids: iterable of str MLflow run IDs
            :param spark: Spark session managing the test data set
            :return: returns a tuple of float evaluation score and str run ID of the best run from the provided iterable
        """
        best_score: float = -1.0
        best_run_id: Optional[str] = None
        for run_id in run_ids:
            score: float = self._evaluate_model(run_id, spark)
            assert score >= 0.0, f"_evaluate_model must return evaluation scores >= 0.0, received {score}"
            if score > best_score:
                best_score = score
                best_run_id = run_id
        return best_score, best_run_id

    def __call__(self, spark: sql.SparkSession, target_stage: str) -> bool:
        """
            Evaluates and promotes model in the MLflow registry.

            :param spark: Spark session managing the test data set
            :param target_stage: str target MLflow Registry stage name to promote to
            :return: returns True if MLflow Registry change was made
        """
        mlflow_client: MlflowClient = MlflowClient()
        cand_run_ids: Iterable[str] = self.experiment_data.where("tags.candidate='true'").select("run_id").toPandas()[
            'run_id'].values
        best_cand_score: float
        best_cand_run_id: Optional[str]
        best_cand_score, best_cand_run_id = self.__get_best_model(cand_run_ids, spark)
        print('Best Score (candidate models): ', best_cand_score)
        try:
            versions: Iterable[ModelVersion] = mlflow_client.get_latest_versions(self.model_name, stages=[target_stage])
            prod_run_ids: List[str] = [v.run_id for v in versions]
            best_prod_score: float
            best_prod_run_id: Optional[str]
            best_prod_score, best_prod_run_id = self.__get_best_model(prod_run_ids, spark)
        except RestException:
            best_prod_score = -1.0
        print('Score (%s models): %s' % (target_stage, best_prod_score))
        promoted: bool = False
        if (best_cand_score >= best_prod_score) and best_cand_run_id:
            # deploy new model
            registered_version: ModelVersion = mlflow.register_model("runs:/" + best_cand_run_id + "/model",
                                                                     self.model_name)
            model_version: Optional[ModelVersion] = None
            for _ in range(12):
                try:
                    model_version = mlflow_client.transition_model_version_stage(name=self.model_name,
                                                                                 version=registered_version.version,
                                                                                 stage=target_stage)
                    break
                except RestException as e:
                    if e.error_code == 'INVALID_STATE_TRANSITION':
                        time.sleep(5)
                    else:
                        raise
            assert model_version is not None, f"Model {self.model_name} has failed to promote to {target_stage} in 1 minute."
            print('Deployed version: ', model_version.version)
            promoted = True
        # remove candidate tags
        for run_id in cand_run_ids:
            mlflow_client.set_tag(run_id, 'candidate', 'false')
        return promoted


try:

    import time
    import mlflow
    import mlflow.tensorflow
    import tensorflow as tf
    from typing import Any, Mapping, Callable, MutableMapping, List
    from pydoc import locate
    from overrides import overrides
    from tensorflow.keras import Model
    from tensorflow.keras.callbacks import EarlyStopping
    from ..data import AbstractTensorflowDataHolder, TensorflowDataProvider
    from ..model import KerasModelProvider


    class KerasTrainStep:
        """
        Pipeline step definition for training a Keras model.
        This is an example class, which may be used as a blueprint for creating training
        steps for other types of models.
        """
        def __init__(self,
                     data_provider: str,
                     model_provider: str,
                     max_epochs: int,
                     patience: int,
                     min_delta: float,
                     **kwargs) -> None:
            """
                Creates a Keras train step definition.

                :param data_provider: str Python class name for an implementation of TensorflowDataProvider
                :param model_provider: str Python class name for an implementation of KerasModelProvider
                :param max_epochs: int maximum number of epochs to run during training
                :param patience: int patience for early stopping
                :param min_delta: float val_loss min delta for early stopping
                :return: returns nothing
            """
            self.model_provider: KerasModelProvider = locate(model_provider)(**kwargs) #type: ignore
            self.data_provider: TensorflowDataProvider = locate(data_provider)(**kwargs) #type: ignore
            self.max_epochs: int = max_epochs
            self.patience: int = patience
            self.min_delta: float = min_delta

        def train(self,
                  train_ds: tf.data.Dataset,
                  train_steps: int,
                  val_ds: tf.data.Dataset,
                  val_steps: int,
                  name: str,
                  train_metadata: Mapping[str, Any] = {}) -> str:
            """
                Trains a Keras model.

                :param train_ds: training data Tensorflow dataset
                :param train_steps: number of training dataset batches per epoch
                :param val_ds: validation data Tensorflow dataset
                :param val_steps: number of validation dataset batches per epoch
                :param name: str MLflow run name
                :param train_metadata: dict of additional MLflow run params
                :return: returns str MLflow run ID
            """
            model: Model = self.model_provider.for_training()
            print(model.summary())
            with mlflow.start_run(run_name=name) as run:
                if train_metadata:
                    mlflow.log_params(train_metadata)
                mlflow.tensorflow.autolog()
                model.fit(train_ds,
                          steps_per_epoch=train_steps,
                          epochs=self.max_epochs,
                          validation_data=val_ds,
                          validation_steps=val_steps,
                          callbacks=[EarlyStopping(patience=self.patience,
                                                   min_delta=self.min_delta)]) #EarlyStopping is needed to grab metrics for MLFlow autolog
                mlflow.set_tag("candidate", "true")
            return run.info.run_id

        def __call__(self, spark: sql.SparkSession, name: str) -> str:
            """
                Trains a Keras model.

                :param spark: Spark session managing the training data set
                :param name: str MLflow run name
                :return: returns str MLflow run ID
            """
            train_holder: AbstractTensorflowDataHolder
            val_holder: AbstractTensorflowDataHolder
            train_ds: tf.data.Dataset
            train_steps: int
            val_ds: tf.data.Dataset
            val_steps: int
            train_holder, val_holder = self.data_provider.for_training(spark)
            with train_holder as (train_ds, train_steps), val_holder as (val_ds, val_steps):
                return self.train(train_ds, train_steps,
                                  val_ds, val_steps,
                                  name,
                                  self.data_provider.train_metadata(spark))


    class KerasEvaluateAndPromoteStep(AbstractEvaluateAndPromoteStep):
        """
        Pipeline step definition for evaluating and promoting a Keras model.
        """
        def __init__(self, data_provider: str, **kwargs) -> None:
            """
                Creates an evaluate step definition.

                :param data_provider: str Python class name for an implementation of TensorflowDataProvider
                :return: returns nothing
            """
            super(KerasEvaluateAndPromoteStep, self).__init__(**kwargs)
            self.data_provider_ctor: Callable[..., TensorflowDataProvider] = locate(data_provider) #type: ignore
            self.kwargs: Mapping[str, Any] = kwargs

        @overrides
        def _evaluate_model(self, run_id: str, spark: sql.SparkSession) -> float:
            """
                Evaluates a Keras model on the test data set using accuracy.

                :param run_id: str MLflow run ID
                :param spark: Spark session managing the test data set
                :return: returns float evaluation score >= 0.0, bigger is better
            """
            try:
                model: Model = mlflow.keras.load_model('runs:/{}/model'.format(run_id))
                print(model.summary())
                kwargs: MutableMapping[str, Any] = dict(self.kwargs)
                kwargs['input_shape'] = model.get_layer(index=0).input_shape[1:]
                with self.data_provider_ctor(**kwargs).for_evaluation(spark) as (test_ds, test_steps):
                    metrics: Mapping[str, float] = model.evaluate(test_ds, steps=test_steps, return_dict=True)
                return metrics['accuracy'] #not "val_accuracy", because evaluation is based on the test dataset
            except Exception as e:
                print(f"Failed to evaluate model for run {run_id}: {e}. Assuming accuracy 0.0.")
                return 0.0

except ImportError as e:
    print(f"Detected import error: {e}. Keras step definitions will not be available.")