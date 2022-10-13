from daily_report_helper import Convert, Email, Graph


def handler(event, context):
    con = Graph.create_connection()
    # Graph.create_directory_for_images()
    graphs = Graph.get_graphs(con)
    graph_names = Graph.get_graph_names()
    Convert.output_graphs_to_png(graphs, graph_names)
    number_of_rides = Graph.get_number_of_rides(con)
    number_of_unique_riders = Graph.get_unique_riders(con)
    report = Convert.get_report(graph_names, number_of_rides, number_of_unique_riders)
    Convert.convert_html_to_pdf(report, '/tmp/report.pdf')
    Email.send_report()



