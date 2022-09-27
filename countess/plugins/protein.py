from fqfa.util.translate import translate_dna

from countess.core.plugins import DaskTranslationPlugin

VERSION = "0.0.1"

class ProteinTranslatorPlugin(DaskTranslationPlugin):

    name = "Protein Translator"
    title = "Translate from DNA to Proteins"
    description = "Translate from DNA to Proteins"
    version = VERSION

    def translate(self, value: str) -> str:
        # Grab all the possible proteins from this DNA ('*' is a stop codon)
        proteins = []
        for f in [0,1,2]:
            try:
                proteins += translate_dna(value, frame=f)[0].split('*')
            except KeyError:
                # XXX some are untranslatable
                pass

        # XXX Just grabs the longest protein translation, which isn't
        # especially likely to be the right one.
        if len(proteins):
            proteins.sort(key=len, reverse=True)
            return proteins[0]
    
        return ""
