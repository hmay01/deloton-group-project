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
send_report('trainee.yusra.anshoor@sigmalabs.co.uk', report)
