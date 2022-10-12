from fqfa.util.translate import translate_dna

from countess.core.plugins import DaskReindexPlugin
from countess.core.parameters import StringCharacterSetParam, IntegerParam

from functools import partial
from Levenshtein import distance

VERSION = "0.0.1"

class ProteinTranslatorPlugin(DaskReindexPlugin):

    name = "Protein Translator"
    title = "Translate from DNA to Proteins"
    description = "Translate from DNA to Proteins"
    version = VERSION

    parameters = {
        'sequence': StringCharacterSetParam("Target Sequence", character_set=set("ABCDEFGHIJKLMNOPQRSTUVWXYZ")),
        'max_distance': IntegerParam("Maximum Levenshtein Distance", 10),
    }

    def translate(self, value: str) -> str:
        # Grab all the possible proteins from this DNA ('*' is a stop codon)
        proteins = []
        for f in [0,1,2]:
            try:
                proteins += translate_dna(value, frame=f)[0].split('*')
            except KeyError:
                # XXX some are untranslatable
                pass


        if len(proteins):
            _distance = partial(distance, self.parameters['sequence'].value)
            proteins.sort(key=_distance)
            print(f"{proteins[0]} {self.parameters['sequence'].value} {_distance(proteins[0])}")
            return proteins[0] if _distance(proteins[0]) <= self.parameters['max_distance'].value else ""
    
        return ""
