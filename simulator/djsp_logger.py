import json
from textwrap import indent
from datetime import datetime, timedelta 

class DJSP_Logger(object):
    def __init__(self, num_job=50, num_machine=5, num_job_type=5):
        self.history = []
        self.jobs_to_schedule = []
        self.num_machine = num_machine
        self.num_job_type = num_job_type
        self.num_job = num_job
        self.order = 0

        self.google_chart_front_text = '''
<html>
<head>
    <script type="text/javascript" src="https://www.gstatic.com/charts/loader.js"></script>
    <script type="text/javascript">
    google.charts.load('current', {'packages':['timeline', 'controls']});
    google.charts.setOnLoadCallback(drawDashboard);

    function drawDashboard() {
        var dataTable = new google.visualization.DataTable();

        dataTable.addColumn({ type: 'string', id: 'Machine' });
        dataTable.addColumn({ type: 'string', id: 'Name' });
        dataTable.addColumn({ type: 'string', role: 'style' });
        dataTable.addColumn({ type: 'string', role: 'tooltip' });
        dataTable.addColumn({ type: 'string', role: 'info_table' });
        dataTable.addColumn({ type: 'string', role: 'tact_table' });
        dataTable.addColumn({ type: 'string', role: 'capacity_table' });
        dataTable.addColumn({ type: 'string', role: 'time_table' });
        dataTable.addColumn({ type: 'string', role: 'model_abbr'});
        dataTable.addColumn({ type: 'date', id: 'Start' });
        dataTable.addColumn({ type: 'date', id: 'End' });
        var scale = 10;
        dataTable.addRows([
    '''

        self.google_chart_back_text = '''
        ]);
        var dashboard = new google.visualization.Dashboard(document.getElementById('dashboard_div'));
        var timeline = new google.visualization.ChartWrapper({
            'chartType': 'Timeline',
            'containerId': 'timeline-tooltip',
            'options':{
                tooltip: { textStyle: { fontName: 'verdana', fontSize: 30 } },
                'width': 1850,
                'height': 500,
                'chartArea': {width: '80%', height: '80%'},
                'backgroundColor': '#ffffff'
            },
            'view': {'columns': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]}
        });
        var control = new google.visualization.ControlWrapper({
            'controlType': 'ChartRangeFilter',
            'containerId': 'filter_div',
            'options': {
                'filterColumnIndex': '9',
                'ui':{
                    'chartType': 'ScatterChart',
                    'chartOptions': {
                        'hAxis': {
                            'title': 'start_time',
                            'gridlines': {'color': 'green', minSpacing: 20},
                            'textPosition': 'out',
                            'viewWindowMode': 'pretty'
                        },
                        'vAxis': {
                            'title': 'end_time',
                            'gridlines': {'color': 'red', minSpacing: 20},
                            'textPosition': 'out',
                            'viewWindowMode': 'pretty'
                        },
                        'pointSize': 5,
                        'width': 2000,
                        'height': 200,
                        'chartArea': {
                            width: '80%',
                            height: '75%'
                        },
                    },
                    'chartView': {'columns': [9, 10]}
                }
            }
          });
        
        dashboard.bind(control, timeline);
        dashboard.draw(dataTable);

        google.visualization.events.addListener(timeline, 'select', function () {
          var selection = dashboard.getSelection();
          if (selection.length > 0) {
            var row = selection[0].row;
            var tooltip = dataTable.getValue(row, 3);
            var info_table = dataTable.getValue(row, 4);
            var tact_table = dataTable.getValue(row, 5);
            var capacity_table = dataTable.getValue(row, 6);
            var time_table = dataTable.getValue(row, 7);
            var model_abbr_table = dataTable.getValue(row, 8);
            document.getElementById('info_table').innerHTML = info_table;
            document.getElementById('info_table').style.display = 'table';
            document.getElementById('tact_table').innerHTML = tact_table;
            document.getElementById('tact_table').style.display = 'table';
            document.getElementById('capacity_table').innerHTML = capacity_table;
            document.getElementById('capacity_table').style.display = 'table';
            document.getElementById('time_table').innerHTML = time_table;
            document.getElementById('time_table').style.display = 'table';
            document.getElementById('model_abbr_table').innerHTML = model_abbr_table;
            document.getElementById('model_abbr_table').style.display = 'table';
          }
        });
      }
    </script>
  </head>
  <body>
    <div id="dashboard_div" style="border: 1px solid #ccc">
        <div id="timeline-tooltip" style="height: 250px;"></div>
        <div id="filter_div"></div>
    </div>
    <div style="overflow-y: scroll; border: 1px solid black;">
        <div id="info_table"><strong>Info table</strong></div>
        <div id="time_table"><strong>Time table</strong></div>
        <div id="model_abbr_table"><strong>Model table</strong></div>
        <div id="tact_table" style="float:left"><strong>Tact table</strong></div>
        <div id="capacity_table" style="float:left"><strong>Capacity table</strong></div>
    </div>
        
    
  </body>
</html>
    '''

    def save(self, json_out_file):
        with open(json_out_file, 'w') as fp:
            json.dump(self.history, fp, indent=4)

    def load(self, json_in_file):
        with open(json_in_file, 'r') as fp:
            self.history = list(json.load(fp))

if __name__ == '__main__':
    logger = DJSP_Logger()
    logger.load('debug.json')
    # print(logger)


