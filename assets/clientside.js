if (!window.dash_clientside) {
    window.dash_clientside = {};
}

window.dash_clientside.clientside = {
    print_table: function (n) {
        if (n > 0) {
            window.myNamespace.tabulator.triggerPrint();
        }
        return null;
    }
}
