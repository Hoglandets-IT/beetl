"""Resources related to running transformers"""

import polars as pl

from .interface import TransformerConfiguration


def run_transformers(
    source: pl.DataFrame,
    transformers: list[TransformerConfiguration],
) -> pl.DataFrame:
    """
    Applies a list of transformers to a source DataFrame.
    """
    transformed = source.clone()

    if transformers is not None and len(transformers) > 0:
        for transformer in transformers:
            if transformer.include_sync:
                transformed = transformer.transform(transformed)
                continue

            transformed = transformer.transform(transformed)

    return transformed
