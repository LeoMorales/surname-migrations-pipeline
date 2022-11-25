from surnames_package.report import Report

def create_report(upstream, product):
    
    report = Report(
        "Migrations", "Using surnames",
        [{
            'title': '',
            'desc': '',
            'value': '',
        }]
    )

    # add section:
    report.add_section(
        '<h2 align="center">Choropleth maps</h2>',
        figure=str(upstream['plot-choroplet-compartive-map'])
    
    )

    # save:
    report.build(str(product))  
