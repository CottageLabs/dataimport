from setuptools import setup, find_packages

setup(
    name='dataimport',
    version='1.0.0',
    packages=find_packages(),
    install_requires=[
        "click~=8.1.3",
        "esprit @ git+https://github.com/CottageLabs/esprit.git@edda12177effa0945d99302f0d453b22503e335b#egg=esprit",
        "requests==2.22.0",
        "Unidecode==1.1.1",
        "Deprecated==1.2.13",
        "Markdown==3.1.1",
        "wheel",
        "PyYAML==6.0",
        "python-slugify==8.0.1"
    ],
    url='https://cottagelabs.com/',
    author='Cottage Labs',
    author_email='us@cottagelabs.com',
    description='Generic, Auditable, Data import tool',
    license='Apache 2.0',
    classifiers=[
        'Operating System :: OS Independent',
        'Programming Language :: Python',
    ],
)
