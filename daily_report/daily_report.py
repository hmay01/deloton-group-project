from daily_report_helper import Convert, Email, Graph



con = Graph.create_connection()
Graph.create_directory_for_images()
graphs = Graph.get_graphs(con)
graph_names = Graph.get_graph_names()
Convert.output_graphs_to_png(graphs, graph_names)
number_of_rides = Graph.get_number_of_rides(con)
report = Convert.get_report(graph_names, number_of_rides)
Convert.convert_html_to_pdf(report, 'report.pdf')
Email.send_report()



