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
Para instalar la ultima version:
```
pip install henka
```
 
Para su uso:
```python
import bar as foo
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

## Ejemplos
Puedes revisar en [`examples/`](examples/)
para diferentes casos de uso.

