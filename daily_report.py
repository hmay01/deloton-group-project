from daily_report_helper import *

con = create_connection()
create_directory_for_images()
graphs = get_graphs(con)
graph_names = get_graph_names()
output_graphs_to_png(graphs, graph_names)
save_all_images_to_bucket(graph_names)
urls = get_all_image_urls(graph_names)
number_of_rides = get_number_of_rides(con)
report = get_report(urls, number_of_rides)
convert_html_to_pdf(report, 'report.pdf')
# send_report('trainee.yusra.anshoor@sigmalabs.co.uk', report)

sender_ = 'trainee.john.andemeskel@sigmalabs.co.uk'
recipients_ = ['trainee.yusra.anshoor@sigmalabs.co.uk']
title_ = 'Deloton Exercise Bikes Daily Report'
text_ = 'Good Afternoon\nDaily reports are attached.'
body_ = """<html><head></head><body><h1>A header 1</h1>
    <br>Good Afternoon.
    <br>Attached is the Daily report pdf. 
    <br>Best wishes
    <br> Yusra stories team
    </html>"""
attachments_ = ['report.pdf']

response_ = send_mail(sender_, recipients_, title_, text_, body_, attachments_)
print(response_)
