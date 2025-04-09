import logging
import string
from functools import lru_cache
from typing import Any, Iterable, Optional

from fqfa.constants.iupac.protein import AA_CODES

from countess import VERSION
from countess.core.parameters import (
    BooleanParam,
    ColumnChoiceParam,
    ColumnOrIntegerParam,
    ColumnOrStringParam,
    DictChoiceParam,
    FramedMultiParam,
    IntegerParam,
    StringCharacterSetParam,
    StringParam,
)
from countess.core.plugins import DuckdbParallelTransformPlugin, DuckdbSqlPlugin
from countess.utils.duckdb import duckdb_escape_identifier, duckdb_escape_literal
from countess.utils.variant import TooManyVariationsException, find_variant_string

logger = logging.getLogger(__name__)

REFERENCE_CHAR_SET = set(string.ascii_uppercase + string.digits + "_")

# XXX Should proabably support these other types as well but I don't
# know what I don't know ...
# XXX Supporting protein calls on mitochondrial (or other organisms)
# DNA will required expansion of the variant caller routine to handle
# different codon tables.  This opens up a can of worms of course.
# XXX There should probably also be a warning generated if you ask for a
# non-MT DNA call with an MT protein call or vice versa.

VARIANT_TYPE_CHOICES = {
    "g": "Genomic (g.)",
    # "o": "Circular Genomic",
    # "m": "Mitochondrial",
    "c": "Coding DNA (c.)",
    "n": "Non-Coding DNA (n.)",
}


class DnaVariantMultiParam(FramedMultiParam):
    prefix = StringCharacterSetParam("Prefix", "", character_set=REFERENCE_CHAR_SET)
    seq_type = DictChoiceParam("Type", choices=VARIANT_TYPE_CHOICES)
    offset = ColumnOrIntegerParam("Offset", 0)
    minus_strand = BooleanParam("Minus Strand", False)
    maxlen = IntegerParam("Max Variations", 10)
    output = StringParam("Output Column", "variant")


class ProteinVariantMultiParam(FramedMultiParam):
    prefix = StringCharacterSetParam("Prefix", "", character_set=REFERENCE_CHAR_SET)
    offset = ColumnOrIntegerParam("Offset", 0)
    # XXX different codon tables go here
    maxlen = IntegerParam("Max Variations", 10)
    output = StringParam("Output Column", "protein")


class VariantPlugin(DuckdbParallelTransformPlugin):
    """Turns a DNA sequence into a HGVS variant code"""

    name = "Variant Caller"
    description = "Turns a DNA sequence into a HGVS variant code"
    version = VERSION
    link = "https://countess-project.github.io/CountESS/included-plugins/#variant-caller"

    column = ColumnChoiceParam("Input Column", "sequence")
    reference = ColumnOrStringParam("Reference Sequence")

    variant = DnaVariantMultiParam("DNA Variant")
    protein = ProteinVariantMultiParam("Protein Variant")

    drop = BooleanParam("Drop unmatched rows", False)
    drop_columns = BooleanParam("Drop Sequence / Reference Columns", False)

    @lru_cache(maxsize=10000)
    def find_variant_string_cached(self, *a, **k):
        return find_variant_string(*a, **k)

    def add_fields(self):
        return {
            self.variant.output.value: str,
            self.protein.output.value: str,
        }

    def remove_fields(self, field_names: list[str]) -> list[Optional[str]]:
        if self.drop_columns:
            return [self.column.value, self.reference.get_column_name()]
        else:
            return []

    def transform(self, data: dict[str, Any]) -> Optional[dict[str, Any]]:
        sequence = data[str(self.column)]
        reference = self.reference.get_value_from_dict(data)
        if not sequence or not reference:
            return None

        # r: dict[str, Any] = data
        if self.variant.output:
            data[self.variant.output.value] = None
            try:
                prefix = self.variant.prefix + ":" if self.variant.prefix else ""
                data[self.variant.output.value] = find_variant_string(
                    f"{prefix}{self.variant.seq_type.get_choice()}.",
                    reference,
                    sequence,
                    max_mutations=self.variant.maxlen.value,
                    offset=int(self.variant.offset.get_value_from_dict(data) or 0),
                    minus_strand=self.variant.minus_strand.value,
                )
            except TooManyVariationsException:
                pass
            except (ValueError, TypeError, KeyError, IndexError) as exc:
                logger.warning("Exception", exc_info=exc)

        if self.protein.output:
            data[self.protein.output.value] = None
            try:
                prefix = self.protein.prefix + ":" if self.protein.prefix else ""
                data[self.protein.output.value] = find_variant_string(
                    f"{prefix}p.",
                    reference,
                    sequence,
                    max_mutations=self.protein.maxlen.value,
                    offset=int(self.protein.offset.get_value_from_dict(data) or 0),
                )
            except TooManyVariationsException:
                pass
            except (ValueError, TypeError, KeyError, IndexError) as exc:
                logger.warning("Exception", exc_info=exc)

        return data


class VariantClassifier(DuckdbSqlPlugin):
    name = "Protein Variant Classifier"
    description = "Classifies protein variants into simple types"
    version = VERSION

    variant_col = ColumnChoiceParam("Protein variant Column", "variant")

    def sql(self, table_name: str, columns: Iterable[str]) -> Optional[str]:
        variant_col_id = duckdb_escape_identifier(self.variant_col.value)
        output_col_id = duckdb_escape_identifier(self.variant_col + "_type")

        # the nested query stops any of the pattern matching columns
        # from colliding with source table columns, plus hopefully
        # duckdb can use the 'group by z' to only match the regex
        # once for each distinct variant string.  Then the cases
        # in the outer select use the parts of the regex match to
        # classify the variant.
        return rf"""
            select S.*, case when T.a != '' or T.c == '' and T.e == '=' then 'W'
               when T.c != '' and (T.c = T.e or T.e = '=') then 'S'
               when T.e = 'Ter' or T.e = '*' then 'N'
               when T.c != '' and T.d != '' and T.e != '' then 'M'
               else '?'
            end as {output_col_id}
            from {table_name} S join (
                select {variant_col_id} as z, unnest(regexp_extract(
                    {variant_col_id},
                    '(_?[Ww][Tt])|(p.)?([A-Z][a-z]*)?(\d+)?([A-Z][a-z]*|[=*])?',
                    ['a','b','c','d','e']
                ))
                from {table_name}
                group by z
            ) T on S.{variant_col_id} = T.z
        """


def _translate_aa(expr: str, expr1: str = "") -> str:
    # This looks ludicrous but it pushes all the work down into SQL so that
    # duckdb can run it without translating rows into Python etc.
    return (
        "CASE "
        + (f"WHEN {expr}={expr1} THEN '='" if expr1 else "")
        + " ".join(
            f"WHEN {expr}={duckdb_escape_literal(k)} THEN {duckdb_escape_literal(v)}"
            for k, v in list(AA_CODES.items()) + [("X", "Ter"), ("-", "del")]
        )
        + " ELSE NULL END"
    )


class VariantConverter(DuckdbSqlPlugin):
    name = "Variant Converter"
    description = "Convert various forms of variant strings to HGVS"
    version = VERSION

    variant_col = ColumnChoiceParam("Variant Column", "variant")
    variant_out = StringParam("Variant Output Column", "variant")
    prefix_aa = StringParam("Variant AA Prefix", "")
    prefix_nt = StringParam("Variant NT Prefix", "")
    offset = IntegerParam("Variant Location Offset", 0)

    def sql(self, table_name: str, columns: Iterable[str]) -> Optional[str]:
        variant_col_id = duckdb_escape_identifier(self.variant_col.value)
        output_col_id = duckdb_escape_identifier(self.variant_out.value)
        offset = duckdb_escape_literal(self.offset.value) if self.offset else 0
        prefix_aa = duckdb_escape_literal((self.prefix_aa.value + ":" if self.prefix_aa else "") + "p.")
        prefix_nt = duckdb_escape_literal((self.prefix_nt.value + ":" if self.prefix_nt else "") + "c.")

        columns = " ".join(duckdb_escape_identifier(c) + "," for c in columns if c != self.variant_out.value)

        # XXX handle other short forms like _synNNNX>Y or whatever
        return rf"""
            SELECT {columns}
            CASE
                WHEN regexp_matches({variant_col_id}, 'wt', 'i') THEN {prefix_aa} || '='
                WHEN regexp_matches({variant_col_id}, '[A-Z]\d+[*A-Z-]')
                    THEN {prefix_aa} || {_translate_aa(variant_col_id + "[1]")} ||
                        ({variant_col_id}[2:-2]::INTEGER + {offset})::TEXT ||
                        {_translate_aa(variant_col_id + "[-1]", variant_col_id + "[1]")}
                WHEN regexp_matches({variant_col_id}, 'syn_\d+[A-Z]>[A-Z]')
                    THEN {prefix_nt} || {variant_col_id}[5:]
                ELSE NULL
            END AS {output_col_id} FROM {table_name}
        """
