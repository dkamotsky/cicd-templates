"""
model module contains definitions of model providers
"""

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
