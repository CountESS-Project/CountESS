from setuptools import setup

setup(
        name = 'countess',
        version = '0.0.1',
        packages = [ 'countess', 'countess.plugins', 'countess.core' ],
        entry_points = {
            'countess_plugins': [
                'load_fastq = countess.plugins.fastq:LoadFastqPlugin',
                'load_hdf = countess.plugins.hdf5:LoadHdfPlugin',
                'do_nothing = countess.plugins.do_nothing:DoNothingPlugin',
                'store_hdf = countess.plugins.hdf5:StoreHdfPlugin',
            ],
            'gui_scripts': ['countess_gui = countess.core.gui:main'],
            'console_scripts': [ 'countess_cmd = countess.core.cmd:main'],
        },
        install_requires = [
            'dask>=2022.8.0',
            'distributed>=2022.8.0',
            'fqfa~=1.2.1',
            'more_itertools~=8.14.0',
            'numpy~=1.23.2',
            'pandas~=1.4.3',
            'tables @ git+https://github.com/PyTables/PyTables.git@da01cf8908c2d8c2b07e8a35685f0811807453f6',
            'pyarrow~=9.0.0',
            'ttkthemes~=3.2.2',
        ]
)

