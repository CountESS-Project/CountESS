# CountESS

This is CountESS, a modular, Python 3 reimplementation of Enrich2.

## License

BSD 3-clause.  See LICENSE.txt

Source code is available at [https://github.com/CountESS-Project/CountESS](https://github.com/CountESS-Project/CountESS)

## Counting with CountESS

To run the CountESS GUI, use `countess_gui`.

Add plugins and set configuration parameters.  Each plugin has its own configuration
tab which shows parameters and below that the abbreviated output of that plugin.

Only the first few hundred rows of input files are read at configuration time.
To perform a complete run using the entire input file, click 'RUN'.

Plugin configurations can be saved and loaded in .INI file format.
CountESS can also be run in headless mode with `countess_cmd your_config.ini`.

## Writing Plugins

CountESS is really just a pipeline of plugins, which are run sequentially to 
process your data.  Some of these are bundled as part of CounteSS or 
available as packages on PyPI, but it is easy to create your own plugin packages.

To write a new plugin:

* make a Python class which inherits from `countess.core.plugins.BasePlugin` or 
  one of its subclasses.
* Fill in `name`, `title`, `description` and `parameters` attributes.
* Implement the appropriate methods (see the examples under core/plugins)
* set you package's `entry_points` to include `countess_plugins` pointing to
  your plugin class.
* install your package locally with `pip install -e .` 

... and your plugin will appear in CountESS.

For more details, see [Writing CountESS Plugins](doc/writing_plugins.md)

## Enquiries

Please contact Alan Rubin `alan@rubin.id.au` for further information or
Nick Moore `nick@zoic.org` for code-related matters.
