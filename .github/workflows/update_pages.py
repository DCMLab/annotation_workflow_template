#!/usr/bin/env python
# coding: utf-8
import argparse
import base64
import io
import os
from fractions import Fraction as frac

import corpusstats
import pandas as pd
import plotly.figure_factory as ff
from ms3 import Parse, make_gantt_data, transform, fifths2name, midi2name, name2fifths, name2pc, resolve_dir
from plotly.offline import plot

# import pandas as pd

INDEX_FNAME = "index.md"
GANTT_FNAME = "gantt.md"
STATS_FNAME = "stats.md"
JEKYLL_CFG_FNAME = "_config.yml"
STYLE_FNAME = "assets/css/style.scss"

INDEX_FILE = f"""
* [Modulation plans]({GANTT_FNAME})
* [Corpus state]({STATS_FNAME})
"""


def generate_stats_text(pie_string, table_string):
    STATS_FILE = f"""
# Corpus Status

## Vital statistics

{table_string}

## Completion ratios

{pie_string}
"""
    return STATS_FILE


JEKYLL_CFG_FILE = "theme: jekyll-theme-tactile "

STYLE_FILE = """---
---

@import "{{ site.theme }}";

.inner {
  max-width: 95%;
  width: 1024px;
}
"""


################################################################################
#                          STYLING GANTT CHARTS
################################################################################


PHRASEEND_LINES = {'color':'rgb(0, 0, 0)','width': 0.2,'dash': 'longdash'}
KEY_COLORS = {'applied':                            'rgb(228,26,28)',
              'local':                              'rgb(55,126,184)',
              'tonic of adjacent applied chord(s)': 'rgb(77,175,74)'}
Y_AXIS = 'Tonicized keys'


def create_modulation_plan(data, task_column='semitones', sort_and_fill=True, title='Modulation plan', globalkey=None, phraseends=None, cadences=None, colors=None):

    if sort_and_fill:
        if task_column in ('semitones', 'fifths'):
            mi, ma = data[task_column].min(), data[task_column].max()
            mi = min((0, mi)) # fifths can be negative
            complete = set(range(mi, ma))
            missing = complete.difference(set(data[task_column]))
            missing_data = pd.DataFrame.from_records([{'Start': 0,
                                                       'Finish': 0,
                                                       'Resource': 'local',
                                                       task_column: m
                                                       }
                                                       for m in missing])
            data = pd.concat([data, missing_data]).sort_values(task_column, ascending=False)
        else:
            # assuming task_column contains strings
            data = data.sort_values(task_column, ascending=False, key=lambda S: S.str.upper())

    if globalkey is not None:
        title += f" ({globalkey})"
        if task_column == 'fifths':
            tonic = name2fifths(globalkey)
            transposed = data.fifths + tonic
            data.fifths = transform(transposed, fifths2name)
        elif task_column == 'semitones':
            tonic = name2pc(globalkey)
            transposed = data.semitones + tonic
            data.semitones = transform(transposed, midi2name)

    ytitle = Y_AXIS
    if task_column in ('semitones', 'fifths'):
        ytitle += f" ({task_column})"


    layout = dict(
        xaxis = {'type': None, 'title': 'Measures'},
        yaxis = {'title': ytitle}
    )

    if colors is None:
        colors = KEY_COLORS

    shapes = None
    if phraseends is not None:
        shapes = [dict(type = 'line',
                       x0 = position,
                       y0 = 0,
                       x1 = position,
                       y1 = 20,
                       line = PHRASEEND_LINES)
                  for position in phraseends]

    #### Old code that needs updating if cadences are to be displayed:
    #### It should append the created lines to the shapes list and the
    #### function create_gantt needs to be expanded for hover items
    # if cadences is not None:
    #     lines = []
    #     annos = []
    #     hover_x = []
    #     hover_y = []
    #     hover_text = []
    #     alt = 0
    #     for i,r in cadences.iterrows():
    #         m = r.m
    #         c = r.type
    #         try:
    #             key = r.key
    #         except:
    #             key = None
    #
    #         if c == 'PAC':
    #             c = 'PC'
    #             w = 1
    #             d = 'solid'
    #         elif c == 'IAC':
    #             c = 'IC'
    #             w = 0.5
    #             d = 'solid'
    #         elif c == 'HC':
    #             w = 0.5
    #             d = 'dash'
    #         elif c in ('EVCAD', 'EC'):
    #             c = 'EC'
    #             w = 0.5
    #             d = 'dashdot'
    #         elif c in ('DEC', 'DC'):
    #             c = 'DC'
    #             w = 0.5
    #             d = 'dot'
    #         else:
    #             print(f"{r.m}: Cadence type {c} unknown.")
    #         #c = c + f"<br>{key}"
    #         linestyle = {'color':'rgb(55, 128, 191)','width': w,'dash':d}
    #         annos.append({'x':m,'y':-0.01+alt*0.03,'font':{'size':7},'showarrow':False,'text':c,'xref':'x','yref':'paper'})
    #         lines.append({'type': 'line','x0':m,'y0':0,'x1':m,'y1':20,'line':linestyle})
    #         alt = 0 if alt else 1
    #         hover_x.append(m)
    #         hover_y.append(-0.5 - alt * 0.5)
    #         text = "Cad: " + r.type
    #         if key is not None:
    #             text += "<br>Key: " + key
    #         text += "<br>Beat: " + str(r.beat)
    #         hover_text.append(text)
    #### The following dictionary represents a new trace that can be added to the
    #### Gantt chart using fig.add_traces([hover_trace])
    # hover_trace=dict(type='scatter',opacity=0,
    #                 x=hover_x,
    #                 y=hover_y,
    #                 marker= dict(size= 14,
    #                             line= dict(width=1),
    #                             color= 'red',
    #                             opacity= 0.3),
    #                 name= "Cadences",
    #                 text= hover_text)

    return create_gantt(data, task_column=task_column, title=title, colors=colors, layout=layout, shapes=shapes)


def create_gantt(data, task_column='Task', title='Gantt chart', colors=None, layout=None, shapes=None, annotations=None, **kwargs):
    """Creates and returns ``fig`` and populates it with features.

    When plotted with plot() or iplot(), ``fig`` shows a Gantt chart.

    Parameters
    ----------
    data: :obj:`pandas.DataFrame`
        DataFrame with at least the columns ['Start', 'Finish', 'Task', 'Resource'].
        Other columns can be selected as 'Task' by passing ``task_column``.
        Further possible columns: 'Description'
    task_column : :obj:`str`, optional
        If ``data`` doesn't have a 'Task' column, pass the name of the column that you want to use as such.
    title: :obj:`str`, optional
        Title to be plotted
    colors : :obj:`dict` or :obj:`str`, optional
        Either a dictionary mapping all occurring values of index_col (default column 'Resource')
        to a color, or the name of the column containing colors. For more options, check out
        :py:meth:`plotly.figure_factory.create_gantt`
    layout : :obj:`dict`
        {key -> dict} which will iteratively update like this: fig['layout'][key].update(dict)
    shapes : :obj:`list` of :obj:`dict`
        One dict per shape that is to be added to the Gantt chart. Dicts typically have the keys
        'type', 'x0', 'y0', 'x1', 'y1', and another keyword for styling the shape.
    annotations : :obj:`list` of :obj:`dict`
        One dict per text annotation that is to be added to the Gantt chart. Dicts typically have the keys
        'x', 'y', 'text', 'font', 'showarrow', 'xref', 'yref'


    **kwargs : Keyword arguments for :py:meth:`plotly.figure_factory.create_gantt`

    Examples
    --------

    >>> iplot(create_gantt(df))

    does the same as

    >>> fig = create_gantt(df)
    >>> iplot(fig)

    To save the chart to a file instead of displaying it directly, use

    >>> plot(fig,filename="filename.html")
    """

    if task_column != 'Task':
        data = data.rename(columns={task_column: 'Task'})

    params = dict(
        group_tasks=True,
        index_col='Resource',
        show_colorbar=True,
        showgrid_x=True,
        showgrid_y=True,
    )
    params.update(kwargs)

    fig = ff.create_gantt(data, colors=colors, title=title, **params)

    # prevent Plotly from interpreting positions as dates
    default_layout = dict(xaxis = {'type': None})
    if layout is not None:
        default_layout.update(layout)

    # if layout is None:
    #     layout = {}
    # if 'xaxis' not in layout:
    #     layout['xaxis'] = {}
    # if 'type' not in layout['xaxis']:
    #     layout['xaxis']['type'] = None

    for key, dictionary in default_layout.items():
        fig['layout'][key].update(dictionary)


    if shapes is not None:
        fig['layout']['shapes'] = fig['layout']['shapes'] + tuple(shapes)

    if annotations is not None:
        fig['layout']['annotations'] = annotations

    #fig['data'].append(hover_trace)     ### older
    #fig.add_traces([hover_trace])       ### newer
    return fig


def get_phraseends(at, column='mn_fraction'):
    """ If make_gantt_data() returned quarterbeats positions, pass column='quarterbeats'
    """
    if column == 'mn_fraction' and 'mn_fraction' not in at.columns:
        mn_fraction = at.mn + (at.mn_onset.astype(float)/at.timesig.map(frac).astype(float))
        at.insert(at.columns.get_loc('mn')+1, 'mn_fraction', mn_fraction)
    return at.loc[at.phraseend.isin([r"\\", "}", "}{"]), column].to_list()


def main(args):
    write_gantt_charts(args)
    write_to_file(args, INDEX_FNAME, INDEX_FILE)
    write_to_file(args, JEKYLL_CFG_FNAME, JEKYLL_CFG_FILE)
    write_to_file(args, STYLE_FNAME, STYLE_FILE)
    write_gantt_file(args)
    write_stats_file(args)


def write_gantt_charts(args):
    p = Parse(
        args.dir,
        paths=args.file,
        file_re=args.regex,
        exclude_re=args.exclude,
        recursive=args.nonrecursive,
        logger_cfg=dict(level=args.level),
    )
    p.parse_mscx()
    gantt_path = (
        check_and_create("gantt")
        if args.out is None
        else check_and_create(os.path.join(args.out, "gantt"))
    )
    for (key, i, _), at in p.get_lists(
        expanded=True
    ).items():  # at stands for annotation table, i.e. DataFrame of expanded labels
        fname = p.fnames[key][i]
        score_obj = p._parsed_mscx[(key, i)]
        metadata = score_obj.mscx.metadata
        logger = score_obj.mscx.logger
        last_mn = metadata["last_mn"]
        globalkey = metadata["annotated_key"]
        logger.debug(f"Creating Gantt data for {fname}...")
        data = make_gantt_data(at)
        phrases = get_phraseends(at)
        data.sort_values(args.yaxis, ascending=False, inplace=True)
        logger.debug(f"Making and storing Gantt chart for {fname}...")
        fig = create_modulation_plan(data, title=f"{fname}", globalkey=globalkey, task_column=args.yaxis, phraseends=phrases)
        out_path = os.path.join(gantt_path, f'{fname}.html')
        plot(fig, filename=out_path)
        logger.debug(f"Stored as {out_path}")


def write_to_file(args, filename, content_str):
    path = check_dir(".") if args.out is None else args.out
    fname = os.path.join(path, filename)
    _ = check_and_create(
        os.path.dirname(fname)
    )  # in case the file name included path components
    with open(fname, "w", encoding="utf-8") as f:
        f.writelines(content_str)


def write_gantt_file(args):
    gantt_path = (
        check_dir("gantt")
        if args.out is None
        else check_dir(os.path.join(args.out, "gantt"))
    )
    fnames = sorted(os.listdir(gantt_path))
    file_content = "\n".join(
        f'<iframe id="igraph" scrolling="no" style="border:none;" seamless="seamless" src="gantt/{f}" height="600" width="100%"></iframe>'
        for f in fnames
    )
    write_to_file(args, GANTT_FNAME, file_content)


def write_stats_file(args):
    p = corpusstats.Provider(args.github, args.token)
    pie_string = ""
    pie_array = []
    for s in p.tabular_stats:
        plot = p.pie_chart(s)
        img = io.BytesIO()
        plot.savefig(img, format="png")
        img.seek(0)
        img = base64.encodebytes(img.getvalue()).decode("utf-8")
        pie_array.append(
            f'<div class="pie_container"><img class="pie" src="data:image/png;base64, {img}"/></div>'
        )
    pie_string = "".join(pie_array)

    vital_stats = pd.DataFrame.from_dict(p.stats, orient="index")
    vital_stats = vital_stats.iloc[0:6, 0:2]
    vital_stats = vital_stats.to_markdown(index=False, headers=[])
    full_text = generate_stats_text(pie_string, vital_stats)
    write_to_file(args, STATS_FNAME, full_text)



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


################################################################################
#                           COMMANDLINE INTERFACE
################################################################################
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="""\
---------------------------------------------------------
| Script for updating GitHub pages for a DCML subcorpus |
---------------------------------------------------------

Description goes here

""",
    )
    parser.add_argument(
        "-g",
        "--github",
        metavar="owner/repository",
        help="If you want to generate corpusstats, you need to pass the repo in the form owner/repository_name and an access token.",
    )
    parser.add_argument(
        "-t",
        "--token",
        metavar="ACCESS_TOKEN",
        help="Token that grants access to the repository in question.",
    )
    parser.add_argument(
        "-d",
        "--dir",
        metavar="DIR",
        nargs="+",
        type=check_dir,
        help="Folder(s) that will be scanned for input files. Defaults to current working directory if no individual files are passed via -f.",
    )
    parser.add_argument(
        "-n",
        "--nonrecursive",
        action="store_false",
        help="Don't scan folders recursively, i.e. parse only files in DIR.",
    )
    parser.add_argument(
        "-f",
        "--file",
        metavar="PATHs",
        nargs="+",
        help="Add path(s) of individual file(s) to be checked.",
    )
    parser.add_argument(
        "-r",
        "--regex",
        metavar="REGEX",
        default=r"\.mscx$",
        help="Select only file names including this string or regular expression. Defaults to MSCX files only.",
    )
    parser.add_argument(
        "-e",
        "--exclude",
        metavar="regex",
        default=r"(^(\.|_)|_reviewed)",
        help="Any files or folders (and their subfolders) including this regex will be disregarded."
        "By default, files including '_reviewed' or starting with . or _ are excluded.",
    )
    parser.add_argument(
        "-o",
        "--out",
        metavar="OUT_DIR",
        type=check_and_create,
        help="""Output directory.""",
    )
    parser.add_argument(
        "-y",
        "--yaxis",
        default="semitones",
        help="Ordering of keys on the y-axis: can be {semitones, fifths, numeral}.",
    )
    parser.add_argument(
        "-l",
        "--level",
        default="INFO",
        help="Set logging to one of the levels {DEBUG, INFO, WARNING, ERROR, CRITICAL}.",
    )
    args = parser.parse_args()
    # logging_levels = {
    #     'DEBUG':    logging.DEBUG,
    #     'INFO':     logging.INFO,
    #     'WARNING':  logging.WARNING,
    #     'ERROR':    logging.ERROR,
    #     'CRITICAL':  logging.CRITICAL,
    #     'D':    logging.DEBUG,
    #     'I':     logging.INFO,
    #     'W':  logging.WARNING,
    #     'E':    logging.ERROR,
    #     'C':  logging.CRITICAL
    #     }
    # logging.basicConfig(level=logging_levels[args.level.upper()])
    if args.file is None and args.dir is None:
        args.dir = os.getcwd()
    main(args)