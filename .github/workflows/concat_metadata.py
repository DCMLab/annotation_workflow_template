#!/usr/bin/env python
# coding: utf-8
import argparse, os, warnings
from typing import Union

import pandas as pd
from pytablewriter import MarkdownTableWriter


BOOLEAN_COLUMNS = ['has_drumset']

def resolve_dir(d):
    """ Resolves '~' to HOME directory and turns ``d`` into an absolute path.
    """
    if d is None:
        return None
    d = str(d)
    if '~' in d:
        return os.path.expanduser(d)
    return os.path.abspath(d)

def check_and_create(d):
    """ Turn input into an existing, absolute directory path.
    """
    if not os.path.isdir(d):
        d = resolve_dir(os.path.join(os.getcwd(), d))
        if not os.path.isdir(d):
            os.makedirs(d)
            print(f"Created directory {d}")
    return resolve_dir(d)


def check_dir(d):
    if not os.path.isdir(d):
        d = resolve_dir(os.path.join(os.getcwd(), d))
        if not os.path.isdir(d):
            raise argparse.ArgumentTypeError(d + " needs to be an existing directory")
    return resolve_dir(d)

def int2bool(s: str) -> Union[bool, str]:
    if isinstance(s, str) and s.lower() in ('t', 'true', 'f', 'false'):
        return s.lower() in ('t', 'true')
    try:
        return bool(int(s))
    except Exception:
        return s

def concat_metadata(path):
    _, folders, _ = next(os.walk(path))
    tsv_paths, keys = [], []
    for subdir in sorted(folders):
        potential = os.path.join(path, subdir, 'metadata.tsv')
        if os.path.isfile(potential):
            tsv_paths.append(potential)
            keys.append(subdir)
    if len(tsv_paths) == 0:
        return pd.DataFrame()
    converters = {col: int2bool for col in BOOLEAN_COLUMNS}
    dfs = [pd.read_csv(tsv_path, sep='\t', dtype='string', converters=converters) for tsv_path in tsv_paths]
    try:
        concatenated = pd.concat(dfs, keys=keys)
    except AssertionError:
        info = 'Levels: ' + ', '.join(f"{key}: {df.index.nlevels} ({df.index.names})" for key, df in zip(keys, dfs))
        print(f"Concatenation of DataFrames failed due to an alignment error. {info}")
        raise
    try:
        rel_path_col = next(col for col in ('subdirectory', 'rel_paths') if col in concatenated.columns)
    except StopIteration:
        raise ValueError(f"Metadata is expected to come with a column called 'subdirectory' or (previously) 'rel_paths'.")
    rel_paths = [os.path.join(corpus, rel_path) for corpus, rel_path in zip(concatenated.index.get_level_values(0), concatenated[rel_path_col].values)]
    concatenated.loc[:, rel_path_col] = rel_paths
    if 'rel_path' in concatenated.columns:
        rel_paths = [os.path.join(corpus, rel_path) for corpus, rel_path in zip(concatenated.index.get_level_values(0), concatenated.rel_path.values)]
        concatenated.loc[:, 'rel_path'] = rel_paths
    concatenated = concatenated.droplevel(1)
    concatenated.index.rename('corpus', inplace=True)
    return concatenated

def df2md(df, name=None):
    """ Turns a DataFrame into a MarkDown table. The returned writer can be converted into a string.
    """
    writer = MarkdownTableWriter()
    writer.table_name = name
    writer.header_list = list(df.columns.values)
    writer.value_matrix = df.values.tolist()
    return writer

def metadata2markdown(concatenated):
    try:
        fname_col = next(col for col in ('fname', 'fnames') if col in concatenated.columns)
    except StopIteration:
        raise ValueError(f"Metadata is expected to come with a column called 'fname' or (previously) 'fnames'.")
    rename4markdown = {
        fname_col: 'file_name',
        'last_mn': 'measures',
        'label_count': 'labels',
        'harmony_version': 'standard',
    }
    concatenated = concatenated.rename(columns=rename4markdown)
    result = '## Overview'
    for corpus_name, df in concatenated[rename4markdown.values()].groupby(level=0):
        heading = f"\n\n### {corpus_name}\n\n"
        md = str(df2md(df.fillna('')))
        result += heading + md
    return result



def write_md(md_str, md_path):
    if os.path.isfile(md_path):
        msg = 'Updated'
        with open(md_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    else:
        msg = 'Created'
        lines = []
    with open(md_path, 'w', encoding='utf-8') as f:
        for line in lines:
            if '# Overview' in line:
                break
            f.write(line)
        else:
            f.write('\n\n')
        f.write(md_str)
    print(f"{msg} {md_path}")

def convert_booleans(df,  bool_columns):
    """"""
    if df is None:
        return df
    boolean_columns = [c for c in bool_columns if c in df.columns]
    if len(boolean_columns) == 0:
        return df
    df = df.copy()
    for bc in boolean_columns:
        null_vals = df[bc].isna()
        if null_vals.all():
            continue
        with warnings.catch_warnings():
            # Setting values in-place is fine, ignore the warning in Pandas >= 1.5.0
            # This can be removed, if Pandas 1.5.0 does not need to be supported any longer.
            # See also: https://stackoverflow.com/q/74057367/859591
            warnings.filterwarnings(
                "ignore",
                category=FutureWarning,
                message=(
                    ".*will attempt to set the values inplace instead of always setting a new array. "
                    "To retain the old behavior, use either.*"
                ),
            )
            try:
                df.loc[:, bc] = df[bc].astype('boolean').astype('Int64')
            except TypeError:
                print(f"COLUMN {bc!r}")
                print([type(val) for val in df[bc]])
                raise
        print(f"Transformed booleans in the column {bc} to integers.")
    return df


def write_tsv(df, tsv_path):
    df = convert_booleans(df, BOOLEAN_COLUMNS)
    df.to_csv(tsv_path, sep='\t', index=True)
    print(f"Concatenated metadata written to {tsv_path}.")
        

def main(args):
    concatenated = concat_metadata(args.dir)
    if len(concatenated) == 0:
        print(f"No metadata found in the child directories of {args.dir}.")
        return
    tsv_path = os.path.join(args.out, 'concatenated_metadata.tsv')
    write_tsv(concatenated, tsv_path)
    md_str = metadata2markdown(concatenated)
    md_path = os.path.join(args.out, 'README.md')
    write_md(md_str, md_path)



################################################################################
#                           COMMANDLINE INTERFACE
################################################################################
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="""\
-------------------------------------------------------------------
| Script for generating metadata and README for meta repositories |
-------------------------------------------------------------------

""",
    )
    parser.add_argument(
        "-d",
        "--dir",
        metavar="DIR",
        type=check_dir,
        help="Pass the root of the repository clone to gather metadata.tsv files from its child directories.",
    )
    parser.add_argument(
        "-o",
        "--out",
        metavar="OUT_DIR",
        type=check_and_create,
        help="""Output directory for TSV and MD file.""",
    )
    args = parser.parse_args()
    if args.dir is None:
        args.dir = os.getcwd()
    if args.out is None:
        args.out = os.getcwd()
    main(args)
