""" Helper functions which find HGVS variants from sequences """

# XXX this should probably become part of MAVE-HGVS library
# https://github.com/VariantEffect/mavehgvs since it is more
# generally applicable than just CountESS!

import re
from typing import Iterable, Optional

from rapidfuzz.distance.Levenshtein import opcodes as levenshtein_opcodes

# Insertions shorter than this won't be searched for, just included.
MIN_SEARCH_LENGTH = 10


def invert_dna_sequence(seq: str) -> str:
    """Invert a DNA sequence: swaps A <-> T and C <-> G and also reverses the direction

    >>> invert_dna_sequence("GATTACA")
    'TGTAATC'
    """

    return seq.translate(str.maketrans("ACGT", "TGCA"))[::-1]


def search_for_sequence(ref_seq: str, var_seq: str, min_search_length: int = MIN_SEARCH_LENGTH) -> str:
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

    It should always pick the 3'-most copy to identify as
    a duplicate

    >>> list(find_variant_dna("ATGGCCGCCAGCCAA", "ATGGCCGCCGCCAGCCAA"))
    ['7_9dup']


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

    >>> list(find_variant_dna("AAAAAAAAAACCCGGGGGGGGGGTTT", "AAAAAAAAAACCCAAAAAAAAAATTT"))
    ['14_23delins1_10']

    DELETION OF NUCLEOTIDES (checked against Enrich2)
    enrich2 has g.5_5del for the first one which isn't correct though

    >>> list(find_variant_dna("AGAAGTAGAGG", "AGAATAGAGG"))
    ['5del']
    >>> list(find_variant_dna("AGAAGTAGAGG", "AGAAAGAGG"))
    ['5_6del']

    INVERSIONS

    >>> list(find_variant_dna("AAACCCTTT", "AAAGGGTTT"))
    ['4_6inv']

    """

    ref_seq = ref_seq.strip().upper()
    var_seq = var_seq.strip().upper()

    if not re.match("[AGTCN]+$", ref_seq):
        raise ValueError("Invalid reference sequence")

    if not re.match("[AGTCN]+$", var_seq):
        raise ValueError("Invalid variant sequence")

    # Levenshtein algorithm finds the overlapping parts of our reference and
    # variant sequences.
    #
    # each element is a text substitution operation on the string of symbols.
    # offsets are python-style whereas HGVS offsets are 1-based and inclusive.
    #
    # 'delete' => delete symbols src_start:src_end
    # 'insert' => insert symbols dest_start:dest_end at src_start
    # 'replace' => replace symbols src_start:src_end with symbols dest_start:dest_end

    opcodes = list(levenshtein_opcodes(ref_seq, var_seq))

    # Sometimes, Levenshtein tries a little too hard to
    # find an "equal" operation between inserts, so this
    # looks for that specific case and fixes it.
    #
    # example: find_variant_string("g.", "ATGGTTGGTTCG", "ATGGTTGGTGGTTC")"
    # before: "g.[9_10insGG;10dup;12del]"
    # after: "g.[7_9dup;12del]"
    #
    # this code recognizes that if there's an "equal" sequence
    # followed by an "insert" of the same sequence, then we
    # can swap them, and *if* there's an "insert" before them
    # then we can reduce the complexity of the output by
    # swapping them and merging the two inserts.
    #
    # XXX can do something similar to turn aligned replace/equal/replace
    # sequences into a single whole-codon replace (delins) as per
    # https://varnomen.hgvs.org/recommendations/DNA/variant/substitution/
    # "two variants separated by one nucleotide, together affecting one
    # amino acid, should be described as a “delins”"
    #
    # example: find_variant_string("c.", "ATGTACAAA", "ATGGATAAA")
    # before: "c.[4T>G;6C>T]"
    # after: "c.[4_6delinsGAT]"

    for n in range(0, len(opcodes) - 2):
        op0, op1, op2 = opcodes[n : n + 3]
        if op0.tag == "insert" and op1.tag == "equal" and op2.tag == "insert":
            seq1 = var_seq[op1.dest_start : op1.dest_end]
            seq2 = var_seq[op2.dest_start : op2.dest_end]
            if seq1 == seq2:
                # extend the first insert and remove the
                # second insert (the following code ignores
                # 'equal's, so it's effectively a NOP)
                op0.dest_end = op1.dest_end
                op2.tag = "equal"

    for opcode in opcodes:
        src_start, src_end = opcode.src_start, opcode.src_end
        src_seq = ref_seq[src_start:src_end]
        dest_seq = var_seq[opcode.dest_start : opcode.dest_end]

        if opcode.tag == "delete":
            assert dest_seq == ""
            # 'delete' opcode maps to HGVS 'del' operation
            if len(src_seq) == 1:
                yield f"{src_start+1}del"
            else:
                yield f"{src_start+1}_{src_end}del"

        elif opcode.tag == "insert":
            assert src_seq == ""
            # 'insert' opcode maps to either an HGVS 'dup' or 'ins' operation

            if ref_seq[src_start - len(dest_seq) : src_start] == dest_seq:
                # This is a duplication of one or more symbols immediately
                # preceding this point.
                if len(dest_seq) == 1:
                    yield f"{src_start}dup"
                else:
                    yield f"{src_start - len(dest_seq) + 1}_{src_start}dup"
            else:
                inserted_sequence = search_for_sequence(ref_seq, dest_seq)
                yield f"{src_start}_{src_start+1}ins{inserted_sequence}"

        elif opcode.tag == "replace":
            # 'replace' opcode maps to either an HGVS '>' (single substitution) or
            # 'inv' (inversion) or 'delins' (delete+insert) operation.

            # XXX does not support "exception: two variants separated by one nucleotide,
            # together affecting one amino acid, should be described as a “delins”",
            # as this code has no concept of amino acid alignment.

            if len(src_seq) == 1 and len(dest_seq) == 1:
                yield f"{src_start+1}{src_seq}>{dest_seq}"
            elif len(src_seq) == len(dest_seq) and dest_seq == invert_dna_sequence(src_seq):
                yield f"{src_start+1}_{src_end}inv"
            else:
                inserted_sequence = search_for_sequence(ref_seq, dest_seq)
                yield f"{src_start+1}_{src_end}delins{inserted_sequence}"


def find_variant_string(prefix: str, ref_seq: str, var_seq: str, max_mutations: Optional[int] = None) -> str:
    """As above, but returns a single string instead of a generator

    MULTIPLE VARIATIONS

    >>> find_variant_string("g.", "GATTACA", "GATTACA")
    'g.='
    >>> find_variant_string("g.", "GATTACA", "GTTTACA")
    'g.2A>T'
    >>> find_variant_string("g.", "GATTACA", "GTTTAGA")
    'g.[2A>T;6C>G]'
    >>> find_variant_string("g.", "GATTACA", "GTTCAGA")
    'g.[2A>T;4T>C;6C>G]'

    >>> find_variant_string("g.", "ATGGTTGGTTC", "ATGGTTGGTGGTTC")
    'g.7_9dup'
    >>> find_variant_string("g.", "ATGGTTGGTTCG", "ATGGTTGGTGGTTC")
    'g.[7_9dup;12del]'
    >>> find_variant_string("g.", "ATGGTTGGTTC", "ATGGTTGGTGGTTCG")
    'g.[7_9dup;11_12insG]'

    CHECK FOR INVALID INPUTS

    >>> find_variant_string("x.", "CAT", "CAT")
    Traceback (most recent call last):
     ...
    ValueError: ...

    >>> find_variant_string("g.", "HELLO", "CAT")
    Traceback (most recent call last):
     ...
    ValueError: Invalid reference sequence

    >>> find_variant_string("g.", "CAT", "HELLO")
    Traceback (most recent call last):
     ...
    ValueError: Invalid variant sequence

    CHECK FOR MAX MUTATIONS

    >>> find_variant_string("g.", "ATTACC", "GATTACA",1)
    Traceback (most recent call last):
     ...
    ValueError: Too many variations...

    """

    if not prefix.endswith("g.") and not prefix.endswith("n."):
        raise ValueError("Only prefix types 'g.' and 'n.' accepted at this time")

    variations = list(find_variant_dna(ref_seq, var_seq))

    if len(variations) == 0:
        return prefix + "="

    if max_mutations is not None and len(variations) > max_mutations:
        raise ValueError("Too many variations (%d) in %s" % (len(variations), var_seq))

    if len(variations) == 1:
        return prefix + variations[0]
    else:
        return prefix + "[" + ";".join(variations) + "]"
