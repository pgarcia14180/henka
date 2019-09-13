[![PyPI Status Badge](https://badge.fury.io/py/tensorflow-addons.svg)](https://cencoreg.cencosud.corp/repository/pyprod/)

# 変化 - Cambio

Libreria Python para manipulacion de dataframe (df) Pandas de manera configurable. 

Puedes renombrar atributos, hacer joins entre varios df, importar/exportar info desde/hacia csv/xls/elasticsearch entre otras funcionalidades.

## Mantenedores
| Subpackage    | Maintainers  | Contact Info                        |
|:----------------------- |:----------- |:----------------------------|
| [faqs](docs/README.md) | FAQS | @ec1363 |


## Instalacion
#### Stable Build
Configuracion de repositorios en ~/.pip/pip.conf:
```
[global]
index-url = https://cencoreg.cencosud.corp/repository/pyprod/simple
trusted-host = cencoreg.cencosud.corp
               pypi.org
               files.pythonhosted.org
extra-index-url= http://pypi.org/simple
                 https://pypi.org/simple
```

Para instalar la ultima version:
```
pip install henka
```

#### Instalacion desde la fuente
Se requiere [Bazel](https://bazel.build/) para generar los componentes.

```
git clone http://gitlab.cencosud.com/pypi-internal/henka.git
cd henka

bazel build henka_pip_pkg
bazel-bin/henka_pip_pkg artifacts

pip install artifacts/henka-*.whl
```

## Uso y Ejemplos
### Uso:
Se debe comenzar importando los componentes:
```python
from henka.config_helpers.henka_config import HenkaConfig
from henka.processes.henka import henka

local_config = [
    {
        # aca van las  configuraciones para df fuentes, joins, reemplazos, destino
    }
]

config = HenkaConfig(*local_config)
henka(config)
```
### Ejemplos:
Puedes revisar en [`examples/`](examples/)
para diferentes casos de uso.

