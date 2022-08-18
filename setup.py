from setuptools import setup

setup(
        name = 'countess',
        version = '0.0.1',
        packages = [ 'countess', 'countess.plugins', 'countess.core' ],
        entry_points = {
            'countess_plugins': [
                'load_hdf = countess.plugins.hdf5:LoadHdfPlugin',
                'do_nothing = countess.plugins.do_nothing:DoNothingPlugin',
                'store_hdf = countess.plugins.hdf5:StoreHdfPlugin',
            ],
            'gui_scripts': ['countess_gui = countess.core.gui:main'],
            'console_scripts': [ 'countess_cmd = countess.core.cmd:main'],
        },
        install_requires = [
            'dask',
            'distributed',
            'numpy',
            'pandas',
            'tables',
            'pyarrow',
            'ttkthemes',
        ]
)

