""" Helper functions which find HGVS variants from sequences """

# XXX this should probably become part of MAVE-HGVS library
# https://github.com/VariantEffect/mavehgvs since it is more
# generally applicable than just CountESS!

import re
from typing import Iterable, Optional

from rapidfuzz.distance.Levenshtein import opcodes

# Insertions shorter than this won't be searched for, just included.
MIN_SEARCH_LENGTH = 10


def invert_dna_sequence(seq: str) -> str:
    """Invert a DNA sequence: swaps A <-> T and C <-> G and also reverses the direction

    >>> invert_dna_sequence("GATTACA")
    'TGTAATC'
    """

    return seq.translate(str.maketrans("ACGT", "TGCA"))[::-1]


def search_for_sequence(
    ref_seq: str, var_seq: str, min_search_length: int = MIN_SEARCH_LENGTH
) -> str:
    """look for a copy of `var_seq` within `ref_seq` and if found return a reference
    in the form "offset1_offset2" otherwise return the `var_seq` itself.  Don't bother
    searching if the length of var_seq is less than MIN_SEARCH_LENGTH.

    >>> search_for_sequence("GGGGGGG", "CAT", 1)
    'CAT'

    >>> search_for_sequence("GGCATGG", "CAT", 1)
    '3_5'

    >>> search_for_sequence("GGATGAA", "CAT", 1)
    '3_5inv'

    >>> search_for_sequence("CATCATC", "CAT", 4)
    'CAT'
    """

    # XXX this could be extended to allow for inverted insertions like `850_900inv`,
    # repeated insertions like `A[26]` and/or complex insertion formats such as
    # `T;450_470;AGGG` but actually finding that kind of match is not simple.

    # XXX consider using something like re.match(r"(.+?)\1+$", var_seq) to search
    # for repeated sequences, but check that it doesn't go O(N!) or whatever
    # (so far on cpython 3.10 this seems fine)

    if len(var_seq) < min_search_length:
        return var_seq

    idx = ref_seq.rfind(var_seq)
    if idx >= 0:
        return f"{idx+1}_{idx+len(var_seq)}"

    inv_seq = invert_dna_sequence(var_seq)
    idx = ref_seq.rfind(inv_seq)
    if idx >= 0:
        return f"{idx+1}_{idx+len(var_seq)}inv"

    return var_seq


def find_variant_dna(ref_seq: str, var_seq: str) -> Iterable[str]:
    """ finds HGVS variants between DNA sequences in ref_seq and var_seq.
    https://varnomen.hgvs.org/recommendations/DNA/variant/insertion/
    Doesn't look for things like complex insertions (yet)

    https://varnomen.hgvs.org/bg-material/numbering/ is pretty clear
    that numbering of nucleotides starts at `1`.

    SUBSTITUTION OF SINGLE NUCLEOTIDES (checked against Enrich2)

    >>> list(find_variant_dna("AGAAGTAGAGG", "TGAAGTAGAGG"))
    ['1A>T']
    >>> list(find_variant_dna("AGAAGTAGAGG", "AGAAGTTGTGG"))
    ['7A>T', '9A>T']
    >>> list(find_variant_dna("AGAAGTAGAGG", "ATAAGAAGAGG"))
    ['2G>T', '6T>A']

    DUPLICATION OF NUCLEOTIDES

    (examples from https://varnomen.hgvs.org/recommendations/DNA/variant/duplication/ )

    >>> list(find_variant_dna(\
        "ATGCTTTGGTGGGAAGAAGTAGAGGACTGTTATGAAAGAGAAGATGTTCAAAAGAA", \
        "ATGCTTTGGTGGGAAGAAGTTAGAGGACTGTTATGAAAGAGAAGATGTTCAAAAGAA"))
    ['20dup']
    >>> list(find_variant_dna(\
        "ATGCTTTGGTGGGAAGAAGTAGAGGACTGTTATGAAAGAGAAGATGTTCAAAAGAA", \
        "ATGCTTTGGTGGGAAGAAGTAGATAGAGGACTGTTATGAAAGAGAAGATGTTCAAAAGAA"))
    ['20_23dup']

    (checked with Enrich2 ... it comes up with `g.6dupT` for the first
    and g.12dupA for the second, but that trailing symbol is not how it
    is in the HGVS docs.  The multi-nucleotide ones it gets very different
    answers for)

    >>> list(find_variant_dna("AGAAGTAGAGG", "AGAAGTTAGAGG"))
    ['6dup']
    >>> list(find_variant_dna("ATTGAAAAAAAATTAG", "ATTGAAAAAAAAATTAG"))
    ['12dup']
    >>> list(find_variant_dna("AGAAGTAGAGG", "AGAAGTAGATAGAGG"))
    ['6_9dup']
    >>> list(find_variant_dna("AAAACTAAAA", "AAAACTCTAAAA"))
    ['5_6dup']
    >>> list(find_variant_dna("AAAACTGAAAA", "AAAACTGCTGAAAA"))
    ['5_7dup']
    >>> list(find_variant_dna("AAAACTGAAAA", "AAAACTGTGAAAA"))
    ['6_7dup']

    INSERTION OF NUCLEOTIDES

    >>> list(find_variant_dna("AGAAGTAGAGG", "AGAAGTCAGAGG"))
    ['6_7insC']
    >>> list(find_variant_dna("AGAAGTAGAGG", "AGAAGTCATAGAGG"))
    ['6_7insCAT']

            0        1
            12345678901234
            AGAAGTAGAGG
            AGAAGTGAAAGAGG
             ^^^  ^^^

    GAA appears earlier in the sequence, but it's too short to bother 
    including as a reference.

    >>> list(find_variant_dna("AGAAGTAGAGG", "AGAAGTGAAAGAGG"))
    ['6_7insGAA']

    the sequence CTTTTTTTTT is long enough to use a reference for.

    >>> list(find_variant_dna("ACTTTTTTTTTAA", "ACTTTTTTTTTACTTTTTTTTTA"))
    ['12_13ins2_11']

    the most 3'ward copy of the sequence should be the one which is 
    referred to.

    >>> list(find_variant_dna(\
        "GGCATCATCATCATGGCATCATCATCATGGGG", \
        "GGCATCATCATCATGGCATCATCATCATGGCATCATCATCATGG"))
    ['30_31ins17_28']

    >>> list(find_variant_dna(\
        "GGCATCATCATCATGGCATCATCATCATGGGGCATCATCATCATGG", \
        "GGCATCATCATCATGGCATCATCATCATGGCATCATCATCATGGCATCATCATCATGG"))
    ['30_31ins33_44']

    (example from Q&A on https://varnomen.hgvs.org/recommendations/DNA/variant/insertion/ )
    "How should I describe the change ATCGATCGATCGATCGAGGGTCCC to
    ATCGATCGATCGATCGAATCGATCGATCGGGTCCC? The fact that the inserted sequence
    (ATCGATCGATCG) is present in the original sequence suggests it derives
    from a duplicative event."
    "The variant should be described as an insertion; g.17_18ins5_16."

            0        1         2         3
            1234567890123456789012345678901234
            ATCGATCGATCGATCGAGGGTCCC
            ATCGATCGATCGATCGAATCGATCGATCGGGTCCC
                ^^^^^^^^^^^  ^^^^^^^^^^^

    right so the duplicated part is 5_15 and its inserted between 17 and 18.
    I think the example in the Q&A is wrong.

    >>> list(find_variant_dna(\
        "ATCGATCGATCGATCGAGGGTCCC", \
        "ATCGATCGATCGATCGAATCGATCGATCGGGTCCC"))
    ['17_18ins5_15']

    DELETION OF NUCLEOTIDES (checked against Enrich2)
    enrich2 has g.5_5del for the first one which isn't correct though

    >>> list(find_variant_dna("AGAAGTAGAGG", "AGAATAGAGG"))
    ['5del']
    >>> list(find_variant_dna("AGAAGTAGAGG", "AGAAAGAGG"))
    ['5_6del']
    """

    ref_seq = ref_seq.strip().upper()
    var_seq = var_seq.strip().upper()

    if not re.match("[AGTC]+$", ref_seq):
        raise ValueError("Invalid reference sequence")

    if not re.match("[AGTC]+$", var_seq):
        raise ValueError("Invalid variant sequence")

    for opcode, ref_from, ref_to, var_from, var_to in opcodes(ref_seq, var_seq).as_list():
        # Levenshtein algorithm finds the overlapping parts of our reference and
        # variant sequences.
        #
        # each element is a text substitution operation on the string of symbols.
        # offsets are python-style whereas HGVS offsets are 1-based and inclusive.
        #
        # ('delete', ref_from, ref_to, n, n) =>
        #     delete symbols ref_from:ref_to from the reference string
        # ('insert', ref_ins, ref_ins, var_from, var_to) =>
        #     insert symbols var_seq[var_from:var_to] into ref_seq[ref_ins]
        # ('replace', ref_from, ref_to, var_from, var_to) =>
        #     replace symbols ref_seq[ref_from:ref_to] with symbols var_seq[var_from:var_to]

        if opcode == "delete":
            # 'delete' opcode maps to HGVS 'del' operation
            if ref_to - ref_from == 1:
                yield f"{ref_from+1}del"
            else:
                yield f"{ref_from+1}_{ref_to}del"

        elif opcode == "insert":
            # 'insert' opcode maps to either an HGVS 'dup' or 'ins' operation
            assert ref_from == ref_to
            var_len = var_to - var_from

            if ref_seq[ref_from - var_len : ref_from] == var_seq[var_from:var_to]:
                # This is a duplication of one or more symbols immediately
                # preceding this point.
                if var_len == 1:
                    yield f"{ref_from}dup"
                else:
                    yield f"{ref_from - var_len + 1}_{ref_from}dup"
            else:
                inserted_sequence = search_for_sequence(ref_seq, var_seq[var_from:var_to])
                yield f"{ref_from}_{ref_from+1}ins{inserted_sequence}"

        elif opcode == "replace":
            # 'replace' opcode maps to either an HGVS '>' (single substitution) or
            # 'inv' (inversion) or 'delins' (delete+insert) operation.

            # XXX does not support "exception: two variants separated by one nucleotide,
            # together affecting one amino acid, should be described as a “delins”",
            # as this code has no concept of amino acid alignment.

            if ref_to - ref_from == 1 and var_to - var_from == 1:
                yield f"{ref_from+1}{ref_seq[ref_from]}>{var_seq[var_from]}"
            elif var_to - var_from == ref_to - ref_from and var_seq[
                var_from:var_to
            ] == invert_dna_sequence(ref_seq[ref_from:ref_to]):
                yield f"{ref_from+1}_{ref_to}inv"
            else:
                inserted_sequence = search_for_sequence(ref_seq, var_seq[var_from:var_to])
                yield f"{ref_from+1}_{ref_to}delins{inserted_sequence}"


def find_variant_string(
    prefix: str, ref_seq: str, var_seq: str, max_mutations: Optional[int] = None
) -> str:
    """As above, but returns a single string instead of a generator"""

    if not prefix.endswith("g.") and not prefix.endswith("n."):
        raise ValueError("Only prefix types 'g.' and 'n.' accepted at this time")

    variations = list(find_variant_dna(ref_seq, var_seq))

    if len(variations) == 0:
        return prefix + "="

    if max_mutations is not None and len(variations) > max_mutations:
        raise ValueError("Too many variations")

    return prefix + "(;)".join(variations)
