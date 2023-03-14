import polars as pl

class Transformer:
    @staticmethod
    def run(fieldName, config: dict, input: pl.DataFrame, output: pl.DataFrame) -> pl.DataFrame:
        # Take the output dataframe, get the columns out and send them through the transformer functions
        output = output.with_column(
            # Extract fields
            pl.struct(config["source"].split(","))
            
            # Run transformer functions with
            # x (KV-dict with source columns)
            # config["transform"] (list of transformers)
            .apply(lambda x: Transformer.runTransform(x, config["transform"]))
            
            # Rename to fieldName
            .alias(fieldName)
        )

        return output

    @staticmethod
    def runTransform(source, transformers):
        print("Run transform")
        field = source
        for transformer in transformers:
            print("Run transformer")
            print(transformer)
            field = getattr(Transformer, transformer['action'])(field, **transformer.get("args", {}))

        return field

    @staticmethod
    def join(data, **args) -> str:
        print("Run join")
        print(data, args)
        return args.get("char", "").join(data.values() if isinstance(data, dict) else data)

    @staticmethod
    def split(data: str, **args) -> list[str]:
        print("Run split")
        print(data, args)
        return data.split(args.get("char", ""))
    
    @staticmethod
    def index(data, **args):
        print("Run index")
        print(data, args)
        index = args.get("index", 0)

        if isinstance(index, (str, list, set)):
            return [data[i] for i in index]
        
        return data[index]

    