from setuptools import setup

setup(
        name = 'CountESS Plugin: Do Nothing',
        version = '0.0.0',
        author = 'CountESS Developers',
        maintainer = 'Nick Moore',
        maintainer_email = 'nick@zoic.org',
        packages = [ '.' ],
        entry_points = {
            'countess_plugins': [
                'do_nothing = do_nothing:DoNothingPlugin',
            ],
        },
        install_requires = [
            'countess>=0.0.1',
        ],
        license_files = ('LICENSE.txt',),
        classifiers = [
            'Intended Audience :: Science/Research',
            'License :: Public Domain',
            'Operating System :: OS Independent',
            'Topic :: Scientific/Engineering :: Bio-Informatics',
        ],
)

