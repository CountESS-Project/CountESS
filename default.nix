{
  python3Packages,
  cmake,
}: let
  fqfa = with python3Packages;
    buildPythonPackage rec {
      pname = "fqfa";
      version = "1.2.3";
      src = fetchPypi {
        inherit pname version;
        sha256 = "sha256-ex0IJfbFWTdFQhXWxtUeH5OQPvaU7LoRnKL9QWyKBos=";
      };
      format = "pyproject";
      nativeBuildInputs = [hatchling];
      pythonImportsCheck = ["fqfa"];
    };

  ttkthemes = with python3Packages;
    buildPythonPackage rec {
      pname = "ttkthemes";
      version = "3.2.2";
      src = fetchPypi {
        inherit pname version;
        sha256 = "sha256-AdrtAB8v8OTzKDKg2epIF2wMUFIDsDB1a93jvRvLIdI=";
      };
      propagatedBuildInputs = [
        tkinter
        pillow
      ];
    };

  blosc2 = with python3Packages;
    buildPythonPackage rec {
      pname = "blosc2";
      version = "2.0.0";
      src = fetchPypi {
        inherit pname version;
        sha256 = "sha256-8ZsLNnT2yCW0kPANgmSwxUDCzcEex+gReNOLg8V3kKE=";
      };
      nativeBuildInputs = [scikit-build cmake cython];
      propagatedBuildInputs = [
        numpy
        msgpack
      ];
      dontUseCmakeConfigure = true;
      format = "pyproject";
    };
in
  with python3Packages;
    buildPythonApplication {
      pname = "CountESS";
      version = "git";
      src = ./.;
      propagatedBuildInputs = [
        dask
        distributed
        fqfa
        more-itertools
        levenshtein
        numpy
        pandas
        (tables.overrideAttrs (attrs: rec {
          version = "3.8.0";
          src = fetchPypi {
            pname = "tables";
            inherit version;
            sha256 = "sha256-NPP6I2bOILGPHfVzp3wdJzBs4fKkHZ+e/2IbUZLqh4g=";
          };
          nativeBuildInputs = attrs.nativeBuildInputs ++ [sphinx];
          propagatedBuildInputs =
            attrs.propagatedBuildInputs
            ++ [
              blosc2
              py-cpuinfo
            ];
        }))
        pyarrow
        ttkthemes
      ];

      nativeBuildInputs = [cython];

      preConfigure = ''
        substituteInPlace setup.py \
          --replace "pyarrow~=10.0.1" "pyarrow"\
          --replace "pandas~=1.5.2" "pandas" # this is 1.5.2 but not detecting version correctly
      '';

      meta = {
        mainProgram = "countess_gui";
      };
    }
