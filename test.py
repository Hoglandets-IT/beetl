from src.beetl import beetl
from src.beetl.transformers.interface import register_transformer

@register_transformer('source', 'test', 'test')
def testTransformer(data, **kwargs):
    data = data.with_columns(data['id'] + 100)
    return data

if __name__ == '__main__':
    cfg = beetl.BeetlConfig.from_yaml_file('tests/static-static.yaml')
    sync = beetl.Beetl(cfg)
    sync.sync()
