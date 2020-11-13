"""
model module contains definitions of model providers
"""

try:

    from abc import ABC, abstractmethod
    from tensorflow.keras import Model


    class KerasModelProvider(ABC):
        """
        Abstract provider of Keras models.
        """
        @abstractmethod
        def for_training(self) -> Model:
            """
                Creates a Keras model.

                :return: Keras model
            """
            raise NotImplementedError

except ImportError as e:
    print(f"Detected import error: {e}. Keras model providers will not be available.")
