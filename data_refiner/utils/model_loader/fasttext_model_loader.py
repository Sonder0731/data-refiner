from typing import Tuple

import fasttext


class FastTextModelLoader:
    """
    A common interface for loading fasttext models.
    """

    def __init__(self, model_path):
        self.model_path = model_path
        self._model = fasttext.load_model(str(self.model_path))

    def predict(self, tokens: str, prefix) -> Tuple[str, float]:
        prediction = self._model.predict(tokens.strip())
        return prediction[0][0].replace(prefix, ""), float(prediction[1])
