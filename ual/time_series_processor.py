import numpy as np
import pandas as pd

from ual.logging import get_logger

log = get_logger("time series processor")

class TimeSeriesProcessor:
    def __init__(self, inputs: pd.DataFrame, targets: pd.DataFrame = None):
        if inputs is not None and inputs.empty:
            log.info("No data in inputs")
        if targets is not None and targets.empty:
            log.info("No data in targets")
        if not isinstance(inputs.index, pd.DatetimeIndex):
            raise TypeError("The inputs index must be a DatetimeIndex.")
        if targets is not None and not isinstance(targets.index, pd.DatetimeIndex):
            raise TypeError("The targets index must be a DatetimeIndex.")

        self.inputs: pd.DataFrame = inputs
        self.targets: pd.DataFrame = targets

    def add_timeseries_feature(self, freq: str, steps: int = 1):
        self.inputs = self._add_timeseries_features(self.inputs, self._get_columns(), freq, steps)
        return self

    def get_inputs(self) -> pd.DataFrame:
        return self.inputs

    def get_targets(self) -> pd.DataFrame:
        return self.targets

    def get_dataframe(self):
        if self.targets is None:
            return self.inputs
        return pd.concat([self.inputs, self.targets], axis=1)

    def _get_columns(self):
        return self.inputs.columns

    @staticmethod
    def _add_timeseries_features(
        data: pd.DataFrame, columns: list[str], freq: str, steps: int = 1
    ) -> pd.DataFrame:
        """Widen the given columns into a small time series: value at t plus t-1..t-steps and t+1..t+steps."""
        offset = pd.tseries.frequencies.to_offset(freq)
        features: dict[str, np.ndarray] = {}
        for column in columns:
            series: pd.Series = data[column]
            for step in range(1, steps + 1):
                # look the neighbours up by timestamp so gaps stay NaN instead of silently shifting rows
                features[f"{column}_t-{step}"] = series.reindex(
                    data.index - step * offset
                ).to_numpy()
                features[f"{column}_t+{step}"] = series.reindex(
                    data.index + step * offset
                ).to_numpy()
        return pd.concat([data, pd.DataFrame(features, index=data.index)], axis=1)
