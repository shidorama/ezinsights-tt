from os.path import abspath as path_abspath
from typing import List, Tuple

from JqPyCharts.main_code import jqpc_simple_bar_chart


def generate_graph(matrix: List[Tuple[str, int, str, str]])-> None:
    """Generates html file with timeseries grpah using

    :param matrix:
    :return:
    """
    html_template = '''
    <!DOCTYPE html>
    <html>
       <head>
    {js_css_resources_header}
    {jqplotchart_script1}
       </head>
       <body>
          <br>
          {html_chart_insert_tag1}
          <br>
       </body>
    </html>
    '''
    height = 15 * len(matrix)
    js_css_resources_header1, jqplotchart_script1, html_chart_insert_tag1 = jqpc_simple_bar_chart(
        absolute_source_dir_path=path_abspath('scripts'),
        script_src_tag_dir_path='scripts',
        chart_id='id_1',
        class_str='',
        chart_title='JqPyCharts Simple Bar Chart: 1 (with defined legends)',
        chart_x_label='',
        chart_x_label_fontdict=None,
        chart_ticks_fontdict=None,
        chart_data_matrix=matrix,
        highlighter_prefix='',
        background='#fffdf6',
        horizontal=True,
        draw_grid_lines=False,
        width_px=1000,
        height_px=height,
        margin_top_px=0,
        margin_bottom_px=0,
        margin_right_px=0,
        margin_left_px=0
    )

    example_final_html_code = html_template.format(
        js_css_resources_header=js_css_resources_header1,
        jqplotchart_script1=jqplotchart_script1,
        html_chart_insert_tag1=html_chart_insert_tag1,
    )

    with open('usage_example__simple_pie_chart.html', 'w') as file_:
        file_.write(example_final_html_code)
