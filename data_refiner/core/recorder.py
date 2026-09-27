from loguru import logger


class Recorder:
    def __init__(self):
        self.dfs = {}

    def record(self, key, df):
        if key in self.dfs:
            logger.warning(f"The dataframe {key} has been covered.")
        self.dfs[key] = df

    def load(self, key):
        if key not in self.dfs:
            raise ValueError(f"No data found for key {key}")
        return self.dfs[key]

    def clear(self):
        self.dfs.clear()


recorder = Recorder()
