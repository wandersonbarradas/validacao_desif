from setuptools import setup, find_packages

setup(
    name="validacao_desif",
    version="0.0.1",
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        'greenlet==3.0.3',
        'polars==0.20.31',
        'PyMySQL==1.1.1',
        'python-dotenv==1.0.1',
        'SQLAlchemy==2.0.30',
        'typing_extensions==4.12.2',
        'python-dateutil~=2.9.0.post0'
    ],
    python_requires='>=3.12',
    author='Wanderson Barradas',
    author_email='wandersonbarrdas07@gmail.com',
)
