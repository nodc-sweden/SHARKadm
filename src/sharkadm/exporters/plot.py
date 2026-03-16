from .. import adm_logger
from ..data import PolarsDataHolder
from ..utils.add_column import add_float_column
from .base import FileExporter

plt = None
sns = None
try:
    import matplotlib.pyplot as plt
    import seaborn as sns
except ModuleNotFoundError as e:
    module_name = str(e).split("'")[-2]
    adm_logger.log_workflow(
        f'Could not import package "{module_name}" in module {__name__}. You need to '
        f"install this dependency if you want to use this module.",
        level=adm_logger.WARNING,
    )


class SimplePlot(FileExporter):
    def __init__(self, xcol: str, zcol: str = "sample_depth_m", **kwargs):
        super().__init__(**kwargs)
        self._xcol = xcol
        self._zcol = zcol

    @staticmethod
    def get_exporter_description() -> str:
        return "Creates a simple plot"

    def _export(self, data_holder: PolarsDataHolder) -> None:
        if not all([plt, sns]):
            adm_logger.log_workflow(
                "Could not create plot. Matplotlib and/or seaborn is missing",
                level=adm_logger.WARNING,
            )
            return
        if not self._export_file_name:
            self._export_file_name = f"plot_{self._xcol}_{self._zcol}.png"

        data = add_float_column(data_holder.data, self._col, column_name="x")
        data = add_float_column(data, self._zcol, column_name="z")
        fig, ax = plt.subplots()
        sns.scatterplot(
            data,
            x="x",
            y="z",
            # hue="species",
            ax=ax,
        )
        ax.set_title("Simple plot")
        ax.set_xlabel(self._xcol)
        ax.set_ylabel(self._zcol)
        fig.savefig(self.export_file_path, format="png")
