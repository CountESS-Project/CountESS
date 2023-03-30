""" Helper functions which find HGVS variants from sequences """

from Levenshtein import opcodes


def find_variant(prefix, ref_seq, var_seq):
    """ finds HGVS variants between DNA sequences in ref_seq and var_seq.
    https://varnomen.hgvs.org/recommendations/DNA/variant/insertion/
    Doesn't look for things like complex insertions (yet)

    https://varnomen.hgvs.org/bg-material/numbering/ is pretty clear
    that numbering of nucleotides starts at `1`.

    SUBSTITUTION OF SINGLE NUCLEOTIDES (checked against Enrich2)

    >>> list(find_variant("g.", "AGAAGTAGAGG", "TGAAGTAGAGG"))
    ['g.1A>T']
    >>> list(find_variant("g.", "AGAAGTAGAGG", "AGAAGTTGTGG"))
    ['g.7A>T', 'g.9A>T']
    >>> list(find_variant("g.", "AGAAGTAGAGG", "ATAAGAAGAGG"))
    ['g.2G>T', 'g.6T>A']

    DUPLICATION OF NUCLEOTIDES (not sure about offsets)

    (examples from https://varnomen.hgvs.org/recommendations/DNA/variant/duplication/ )

    >>> list(find_variant("g.", \
        "ATGCTTTGGTGGGAAGAAGTAGAGGACTGTTATGAAAGAGAAGATGTTCAAAAGAA", \
        "ATGCTTTGGTGGGAAGAAGTTAGAGGACTGTTATGAAAGAGAAGATGTTCAAAAGAA"))
    ['g.20dup']
    >>> list(find_variant("g.", \
        "ATGCTTTGGTGGGAAGAAGTAGAGGACTGTTATGAAAGAGAAGATGTTCAAAAGAA", \
        "ATGCTTTGGTGGGAAGAAGTAGATAGAGGACTGTTATGAAAGAGAAGATGTTCAAAAGAA"))
    ['g.20_23dup']

    (checked with Enrich2 ... it comes up with `g.6dupT` for the first
    and g.12dupA for the second, but that trailing symbol is not how it
    is in the HGVS docs.  The multi-nucleotide ones it gets very different
    answers for)

    >>> list(find_variant("g.", "AGAAGTAGAGG", "AGAAGTTAGAGG"))
    ['g.6dup']
    >>> list(find_variant("g.", "ATTGAAAAAAAATTAG", "ATTGAAAAAAAAATTAG"))
    ['g.12dup']
    >>> list(find_variant("g.", "AGAAGTAGAGG", "AGAAGTAGATAGAGG"))
    ['g.6_9dup']
    >>> list(find_variant("g.", "AAAACTAAAA", "AAAACTCTAAAA"))
    ['g.5_6dup']
    >>> list(find_variant("g.", "AAAACTGAAAA", "AAAACTGCTGAAAA"))
    ['g.5_7dup']
    >>> list(find_variant("g.", "AAAACTGAAAA", "AAAACTGTGAAAA"))
    ['g.6_7dup']


    INSERTION OF NUCLEOTIDES

    >>> list(find_variant("g.", "AGAAGTAGAGG", "AGAAGTCAGAGG"))
    ['g.6_7insC']
    >>> list(find_variant("g.", "AGAAGTAGAGG", "AGAAGTCATAGAGG"))
    ['g.6_7insCAT']

            0        1
            12345678901234
            AGAAGTAGAGG
            AGAAGTGAAAGAGG
             ^^^  ^^^

    GAA appears earlier in the sequence, but it's too short to bother 
    including as a reference.

    >>> list(find_variant("g.", "AGAAGTAGAGG", "AGAAGTGAAAGAGG"))
    ['g.6_7insGAA']

    the sequence CTTTTTTTTT is long enough to use a reference for.

    >>> list(find_variant("g.", "ACTTTTTTTTTAA", "ACTTTTTTTTTACTTTTTTTTTA"))
    ['g.12_13ins2_11']

    the most 3'ward copy of the sequence should be the one which is 
    referred to.

    >>> list(find_variant("g.", \
        "GGCATCATCATCATGGCATCATCATCATGGGG", \
        "GGCATCATCATCATGGCATCATCATCATGGCATCATCATCATGG"))
    ['g.30_31ins17_28']

    >>> list(find_variant("g.", \
        "GGCATCATCATCATGGCATCATCATCATGGGGCATCATCATCATGG", \
        "GGCATCATCATCATGGCATCATCATCATGGCATCATCATCATGGCATCATCATCATGG"))
    ['g.30_31ins33_44']

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

    >>> list(find_variant("g.", \
        "ATCGATCGATCGATCGAGGGTCCC", \
        "ATCGATCGATCGATCGAATCGATCGATCGGGTCCC"))
    ['g.17_18ins5_15']

    DELETION OF NUCLEOTIDES (checked against Enrich2)
    enrich2 has g.5_5del for the first one which isn't correct though

    >>> list(find_variant("g.", "AGAAGTAGAGG", "AGAATAGAGG"))
    ['g.5del']
    >>> list(find_variant("g.", "AGAAGTAGAGG", "AGAAAGAGG"))
    ['g.5_6del']
    """

    for opc, ref_from, ref_to, var_from, var_to in opcodes(ref_seq, var_seq):
        if opc == 'delete':
            if ref_to == ref_from + 1:
                yield f"{prefix}{ref_from+1}del"
            else:
                yield f"{prefix}{ref_from+1}_{ref_to}del"
        elif opc == 'insert':
            assert ref_from == ref_to
            seq = var_seq[var_from:var_to]
            dup_from = ref_from - (var_to - var_from)
            if ref_seq[dup_from:ref_from] == seq:
                # This is a duplication of one or more codes
                if ref_from - dup_from == 1:
                    yield f"{prefix}{dup_from+1}dup"
                else:
                    yield f"{prefix}{dup_from+1}_{ref_from}dup"
            else:
                # This is an insertion. If it's short, just include it.
                if len(seq) < 10:
                    yield f"{prefix}{ref_from}_{ref_from+1}ins{seq}"
                else:
                    try:
                        ref_find = ref_seq.rindex(seq)
                        # match found, refer to it.
                        yield f"{prefix}{ref_from}_{ref_from+1}ins{ref_find+1}_{ref_find+len(seq)}"
                    except ValueError:
                        # no match found, dump the whole sequence
                        yield f"{prefix}{ref_from}_{ref_from+1}ins{seq}"
        elif opc == 'replace':
            if ref_to - ref_from == 1 and var_to - var_from == 1:
                seq = var_seq[var_from:var_to]
                yield f"{prefix}{ref_from+1}{ref_seq[ref_from]}>{seq}"
            else:
                yield f"{prefix}{ref_from+1}{ref_seq[ref_from]}>{var_seq[var_from]}"


def find_variant_string(prefix, ref_seq, var_seq):
    """As above, but returns a single string instead of a generator"""
    return prefix + '(;)'.join(find_variant('', ref_seq, var_seq))
