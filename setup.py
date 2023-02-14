from setuptools import setup
from countess import VERSION

setup(
        name = 'countess',
        version = VERSION,
        author = 'CountESS Developers',
        maintainer = 'Nick Moore',
        maintainer_email = 'nick@zoic.org',
        packages = [ 'countess', 'countess.utils', 'countess.plugins', 'countess.core', 'countess.gui' ],
        entry_points = {
            'countess_plugins': [
                'load_fastq = countess.plugins.fastq:LoadFastqPlugin',
                'load_hdf = countess.plugins.hdf5:LoadHdfPlugin',
                'load_csv = countess.plugins.csv:LoadCsvPlugin',
                'log_score = countess.plugins.log_score:LogScorePlugin',
                'group_by = countess.plugins.group_by:GroupByPlugin',
                #'protein = countess.plugins.protein:ProteinTranslatorPlugin',
                #'embed_r = countess.plugins.embed_r:EmbeddedRPlugin',
                'embed_py = countess.plugins.embed_python:EmbeddedPythonPlugin',
                #'regex_reader = countess.plugins.regex_reader:RegexReaderPlugin',
                'pivot = countess.plugins.pivot:DaskPivotPlugin',
                'join = countess.plugins.join:DaskJoinPlugin',
                'save_csv = countess.plugins.csv:SaveCsvPlugin',
                'regex_tool = countess.plugins.regex_tool:RegexToolPlugin',
            ],
            'gui_scripts': ['countess_gui = countess.gui.main:main'],
            'console_scripts': [ 'countess_cmd = countess.core.cmd:main'],
        },
        install_requires = [
            'dask>=2022.8.0',
            'distributed>=2022.8.0',
            'fqfa~=1.2.1',
            'more_itertools~=8.14.0',
            'Levenshtein==0.20.5',
            'numpy~=1.23.2',
            'pandas~=1.5.2',
            'tables==3.8.0',
            'pyarrow~=10.0.1',
            'ttkthemes~=3.2.2',
        ],
        #extras_require = {
        #    'r': [
        #        'rpy2==3.5.1'
        #    ],
        #},
        license = 'BSD',
        license_files = ('LICENSE.txt',),
        classifiers = [
            'Development Status :: 2 - Pre-Alpha',
            'Intended Audience :: Science/Research',
            'License :: OSI Approved :: BSD License',
            'Operating System :: OS Independent',
            'Topic :: Scientific/Engineering :: Bio-Informatics',
        ],
        long_description = "Constructs pipelines of plugins for bioinformations data processing",
        long_description_content_type = "text/x-rst",
)

