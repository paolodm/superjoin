(function() {
    var MainViewModel;

    MainViewModel = (function() {

        function MainViewModel() {
            this.slices = ko.observableArray([]);
            this.slice1 = ko.observable();
            this.slice2 = ko.observable();


            this.join = function() {
                $.ajax({
                    url: '/join',
                    data: {
                        slice1: this.slice1(),
                        slic32: this.slice2()
                    }

                });
            }
        }

        return MainViewModel;

    })();

    window.MainViewModel = MainViewModel;

}).call(this);
