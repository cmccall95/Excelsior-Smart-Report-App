window.dash_clientside = Object.assign({}, window.dash_clientside, {
    my_clientside_namespace: {
        printTable: function (n_clicks) {
            console.log("Print Activated")

            if (n_clicks > 0) {
                // Retrieve the JSON data from dcc.Store
                var data = JSON.parse(document.getElementById('rt-data-store').textContent);
                console.log("data:")
                console.log(data)

                if (data) {
                    // Generate an HTML table string from the JavaScript object
                    var output = '<table border="1">';
                    var keys = Object.keys(data[0]);
                    output += '<tr>';
                    keys.forEach(key => {
                        output += '<th>' + key + '</th>';
                    });
                    output += '</tr>';

                    data.forEach(row => {
                        output += '<tr>';
                        keys.forEach(key => {
                            output += '<td>' + row[key] + '</td>';
                        });
                        output += '</tr>';
                    });
                    output += '</table>';

                    console.log("output:")
                    console.log(output)

                    // Open a new window and write the HTML table string into it, and print
                    var printWindow = window.open('', '_blank');
                    printWindow.document.write('<html><head><title>Print</title></head><body>');
                    printWindow.document.write(output);
                    printWindow.document.write('</body></html>');
                    printWindow.document.close();
                    printWindow.print();

                } else {
                    console.log("No data found.");
                }
            }
        }
    }
});
