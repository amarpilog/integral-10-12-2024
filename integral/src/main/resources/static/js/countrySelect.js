// wrap in UMD - see https://github.com/umdjs/umd/blob/master/jqueryPlugin.js
(function (factory) {
    if (typeof define === "function" && define.amd) {
        define(["jquery"], function ($) {
            factory($, window, document);
        });
    } else if (typeof module === "object" && module.exports) {
        module.exports = factory(require("jquery"), window, document);
    } else {
        factory(jQuery, window, document);
    }
})(function ($, window, document, undefined) {
    "use strict";
    var pluginName = "countrySelect", id = 1, // give each instance its own ID for namespaced event handling
            defaults = {
                // Default country
                defaultCountry: "",
                // Position the selected flag inside or outside of the input
                defaultStyling: "inside",
                // don't display these countries
                excludeCountries: [],
                // Display only these countries
                onlyCountries: [],
                // The countries at the top of the list. Defaults to United States and United Kingdom
                preferredCountries: ["us", "gb"],
                // localized country names e.g. { 'de': 'Deutschland' }
                localizedCountries: null,
                // Set the dropdown's width to be the same as the input. This is automatically enabled for small screens.
                responsiveDropdown: ($(window).width() < 768 ? true : false),
            }, keys = {
        UP: 38,
        DOWN: 40,
        ENTER: 13,
        ESC: 27,
        BACKSPACE: 8,
        PLUS: 43,
        SPACE: 32,
        A: 65,
        Z: 90
    }, windowLoaded = false;
    // keep track of if the window.load event has fired as impossible to check after the fact
    $(window).on('load', function () {
        windowLoaded = true;
    });
    function Plugin(element, options) {
        this.element = element;
        this.options = $.extend({}, defaults, options);
        this._defaults = defaults;
        // event namespace
        this.ns = "." + pluginName + id++;
        this._name = pluginName;
        this.init();
    }
    Plugin.prototype = {
        init: function () {
            // Process all the data: onlyCountries, excludeCountries, preferredCountries, defaultCountry etc
            this._processCountryData();
            // Generate the markup
            this._generateMarkup();
            // Set the initial state of the input value and the selected flag
            this._setInitialState();
            // Start all of the event listeners: input keyup, selectedFlag click
            this._initListeners();
            // Return this when the auto country is resolved.
            this.autoCountryDeferred = new $.Deferred();
            // Get auto country.
            this._initAutoCountry();
            // Keep track as the user types
            this.typedLetters = "";

            return this.autoCountryDeferred;
        },
        /********************
         *  PRIVATE METHODS
         ********************/
        // prepare all of the country data, including onlyCountries, excludeCountries, preferredCountries and
        // defaultCountry options
        _processCountryData: function () {
            // set the instances country data objects
            this._setInstanceCountryData();
            // set the preferredCountries property
            this._setPreferredCountries();
            // translate countries according to localizedCountries option
            if (this.options.localizedCountries)
                this._translateCountriesByLocale();
            // sort countries by name
            if (this.options.onlyCountries.length || this.options.localizedCountries) {
                this.countries.sort(this._countryNameSort);
            }
        },
        // process onlyCountries array if present
        _setInstanceCountryData: function () {
            var that = this;
            if (this.options.onlyCountries.length) {
                var newCountries = [];
                $.each(this.options.onlyCountries, function (i, countryCode) {
                    var countryData = that._getCountryData(countryCode, true);
                    if (countryData) {
                        newCountries.push(countryData);
                    }
                });
                this.countries = newCountries;
            } else if (this.options.excludeCountries.length) {
                var lowerCaseExcludeCountries = this.options.excludeCountries.map(function (country) {
                    return country.toLowerCase();
                });
                this.countries = allCountries.filter(function (country) {
                    return lowerCaseExcludeCountries.indexOf(country.iso2) === -1;
                });
            } else {
                this.countries = allCountries;
            }
        },
        // Process preferred countries - iterate through the preferences,
        // fetching the country data for each one
        _setPreferredCountries: function () {
            var that = this;
            this.preferredCountries = [];
            $.each(this.options.preferredCountries, function (i, countryCode) {
                var countryData = that._getCountryData(countryCode, false);
                if (countryData) {
                    that.preferredCountries.push(countryData);
                }
            });
        },
        // Translate Countries by object literal provided on config
        _translateCountriesByLocale() {
            for (let i = 0; i < this.countries.length; i++) {
                const iso = this.countries[i].iso2.toLowerCase();
                if (this.options.localizedCountries.hasOwnProperty(iso)) {
                    this.countries[i].name = this.options.localizedCountries[iso];
                }
            }
        },
        // sort by country name
        _countryNameSort(a, b) {
            return a.name.localeCompare(b.name);
        },
        // generate all of the markup for the plugin: the selected flag overlay, and the dropdown
        _generateMarkup: function () {
            // Country input
            this.countryInput = $(this.element);
            // containers (mostly for positioning)
            var mainClass = "country-select";
            if (this.options.defaultStyling) {
                mainClass += " " + this.options.defaultStyling;
            }
            this.countryInput.wrap($("<div>", {
                "class": mainClass
            }));
            var flagsContainer = $("<div>", {
                "class": "flag-dropdown"
            }).insertAfter(this.countryInput);
            // currently selected flag (displayed to left of input)
            var selectedFlag = $("<div>", {
                "class": "selected-flag"
            }).appendTo(flagsContainer);
            this.selectedFlagInner = $("<div>", {
                "class": "flag"
            }).appendTo(selectedFlag);
            // CSS triangle
            $("<div>", {
                "class": "arrow"
            }).appendTo(selectedFlag);
            // country list contains: preferred countries, then divider, then all countries
            this.countryList = $("<ul>", {
                "class": "country-list v-hide"
            }).appendTo(flagsContainer);
            if (this.preferredCountries.length) {
                this._appendListItems(this.preferredCountries, "preferred");
                $("<li>", {
                    "class": "divider"
                }).appendTo(this.countryList);
            }
            this._appendListItems(this.countries, "");
            // Add the hidden input for the country code
            this.countryCodeInput = $("#" + this.countryInput.attr("id") + "_code");
            if (!this.countryCodeInput) {
                this.countryCodeInput = $('<input type="hidden" id="' + this.countryInput.attr("id") + '_code" name="' + this.countryInput.attr("name") + '_code" value="" />');
                this.countryCodeInput.insertAfter(this.countryInput);
            }
            // now we can grab the dropdown height, and hide it properly
            this.dropdownHeight = this.countryList.outerHeight();
            // set the dropdown width according to the input if responsiveDropdown option is present or if it's a small screen
            if (this.options.responsiveDropdown) {
                $(window).resize(function () {
                    $('.country-select').each(function () {
                        var dropdownWidth = this.offsetWidth;
                        $(this).find('.country-list').css("width", dropdownWidth + "px");
                    });
                }).resize();
            }
            this.countryList.removeClass("v-hide").addClass("hide");
            // this is useful in lots of places
            this.countryListItems = this.countryList.children(".country");
        },
        // add a country <li> to the countryList <ul> container
        _appendListItems: function (countries, className) {
            // Generate DOM elements as a large temp string, so that there is only
            // one DOM insert event
            var tmp = "";
            // for each country
            $.each(countries, function (i, c) {
                // open the list item
                tmp += '<li class="country ' + className + '" data-country-code="' + c.iso2 + '">';
                // add the flag
                tmp += '<div class="flag ' + c.iso2 + '"></div>';
                // and the country name
                tmp += '<span class="country-name" data-dialcode="' + c.stdC + '">' + c.name + '</span>';
                // close the list item
                tmp += '</li>';
            });
            this.countryList.append(tmp);
        },
        // set the initial state of the input value and the selected flag
        _setInitialState: function () {
            var flagIsSet = false;
            // If the input is pre-populated, then just update the selected flag
            if (this.countryInput.val()) {
                flagIsSet = this._updateFlagFromInputVal();
            }
            // If the country code input is pre-populated, update the name and the selected flag
            var selectedCode = this.countryCodeInput.val();
            if (selectedCode) {
                this.selectCountry(selectedCode);
            }
            if (!flagIsSet) {
                // flag is not set, so set to the default country
                var defaultCountry;
                // check the defaultCountry option, else fall back to the first in the list
                if (this.options.defaultCountry) {
                    defaultCountry = this._getCountryData(this.options.defaultCountry, false);
                    // Did we not find the requested default country?
                    if (!defaultCountry) {
                        defaultCountry = this.preferredCountries.length ? this.preferredCountries[0] : this.countries[0];
                    }
                } else {
                    defaultCountry = this.preferredCountries.length ? this.preferredCountries[0] : this.countries[0];
                }
                this.defaultCountry = defaultCountry.iso2;
            }
        },
        // initialise the main event listeners: input keyup, and click selected flag
        _initListeners: function () {
            var that = this;
            // Update flag on keyup.
            // Use keyup instead of keypress because we want to update on backspace
            // and instead of keydown because the value hasn't updated when that
            // event is fired.
            // NOTE: better to have this one listener all the time instead of
            // starting it on focus and stopping it on blur, because then you've
            // got two listeners (focus and blur)
            this.countryInput.on("keyup" + this.ns, function () {
                that._updateFlagFromInputVal();
            });
            // toggle country dropdown on click
            var selectedFlag = this.selectedFlagInner.parent();
            selectedFlag.on("click" + this.ns, function (e) {
                // only intercept this event if we're opening the dropdown
                // else let it bubble up to the top ("click-off-to-close" listener)
                // we cannot just stopPropagation as it may be needed to close another instance
                if (that.countryList.hasClass("hide") && !that.countryInput.prop("disabled")) {
                    that._showDropdown();
                }
            });
            // Despite above note, added blur to ensure partially spelled country
            // with correctly chosen flag is spelled out on blur. Also, correctly
            // selects flag when field is autofilled
            this.countryInput.on("blur" + this.ns, function () {
                if (that.countryInput.val() != that.getSelectedCountryData().name) {
                    that.setCountry(that.countryInput.val());
                }
                that.countryInput.val(that.getSelectedCountryData().name);
            });
        },
        _initAutoCountry: function () {
            if (this.options.initialCountry === "auto") {
                this._loadAutoCountry();
            } else {
                if (this.defaultCountry) {
                    this.selectCountry(this.defaultCountry);
                }
                this.autoCountryDeferred.resolve();
            }
        },
        // perform the geo ip lookup
        _loadAutoCountry: function () {
            var that = this;

            // 3 options:
            // 1) already loaded (we're done)
            // 2) not already started loading (start)
            // 3) already started loading (do nothing - just wait for loading callback to fire)
            if ($.fn[pluginName].autoCountry) {
                this.handleAutoCountry();
            } else if (!$.fn[pluginName].startedLoadingAutoCountry) {
                // don't do this twice!
                $.fn[pluginName].startedLoadingAutoCountry = true;

                if (typeof this.options.geoIpLookup === 'function') {
                    this.options.geoIpLookup(function (countryCode) {
                        $.fn[pluginName].autoCountry = countryCode.toLowerCase();
                        // tell all instances the auto country is ready
                        // TODO: this should just be the current instances
                        // UPDATE: use setTimeout in case their geoIpLookup function calls this callback straight away (e.g. if they have already done the geo ip lookup somewhere else). Using setTimeout means that the current thread of execution will finish before executing this, which allows the plugin to finish initialising.
                        setTimeout(function () {
                            $(".country-select input").countrySelect("handleAutoCountry");
                        });
                    });
                }
            }
        },
        // Focus input and put the cursor at the end
        _focus: function () {
            this.countryInput.focus();
            var input = this.countryInput[0];
            // works for Chrome, FF, Safari, IE9+
            if (input.setSelectionRange) {
                var len = this.countryInput.val().length;
                input.setSelectionRange(len, len);
            }
        },
        // Show the dropdown
        _showDropdown: function () {
            this._setDropdownPosition();
            // update highlighting and scroll to active list item
            var activeListItem = this.countryList.children(".active");
            this._highlightListItem(activeListItem);
            // show it
            this.countryList.removeClass("hide");
            this._scrollTo(activeListItem);
            // bind all the dropdown-related listeners: mouseover, click, click-off, keydown
            this._bindDropdownListeners();
            // update the arrow
            this.selectedFlagInner.parent().children(".arrow").addClass("up");
        },
        // decide where to position dropdown (depends on position within viewport, and scroll)
        _setDropdownPosition: function () {
            var inputTop = this.countryInput.offset().top, windowTop = $(window).scrollTop(),
                    dropdownFitsBelow = inputTop + this.countryInput.outerHeight() + this.dropdownHeight < windowTop + $(window).height(), dropdownFitsAbove = inputTop - this.dropdownHeight > windowTop;
            // dropdownHeight - 1 for border
            var cssTop = !dropdownFitsBelow && dropdownFitsAbove ? "-" + (this.dropdownHeight - 1) + "px" : "";
            this.countryList.css("top", cssTop);
        },
        // we only bind dropdown listeners when the dropdown is open
        _bindDropdownListeners: function () {
            var that = this;
            // when mouse over a list item, just highlight that one
            // we add the class "highlight", so if they hit "enter" we know which one to select
            this.countryList.on("mouseover" + this.ns, ".country", function (e) {
                that._highlightListItem($(this));
            });
            // listen for country selection
            this.countryList.on("click" + this.ns, ".country", function (e) {
                that._selectListItem($(this));
            });
            // click off to close
            // (except when this initial opening click is bubbling up)
            // we cannot just stopPropagation as it may be needed to close another instance
            var isOpening = true;
            $("html").on("click" + this.ns, function (e) {
                e.preventDefault();
                if (!isOpening) {
                    that._closeDropdown();
                }
                isOpening = false;
            });
            // Listen for up/down scrolling, enter to select, or letters to jump to country name.
            // Use keydown as keypress doesn't fire for non-char keys and we want to catch if they
            // just hit down and hold it to scroll down (no keyup event).
            // Listen on the document because that's where key events are triggered if no input has focus
            $(document).on("keydown" + this.ns, function (e) {
                // prevent down key from scrolling the whole page,
                // and enter key from submitting a form etc
                e.preventDefault();
                if (e.which == keys.UP || e.which == keys.DOWN) {
                    // up and down to navigate
                    that._handleUpDownKey(e.which);
                } else if (e.which == keys.ENTER) {
                    // enter to select
                    that._handleEnterKey();
                } else if (e.which == keys.ESC) {
                    // esc to close
                    that._closeDropdown();
                } else if (e.which >= keys.A && e.which <= keys.Z || e.which === keys.SPACE) {
                    that.typedLetters += String.fromCharCode(e.which);
                    that._filterCountries(that.typedLetters);
                } else if (e.which === keys.BACKSPACE) {
                    that.typedLetters = that.typedLetters.slice(0, -1);
                    that._filterCountries(that.typedLetters);
                }
            });
        },
        // Highlight the next/prev item in the list (and ensure it is visible)
        _handleUpDownKey: function (key) {
            var current = this.countryList.children(".highlight").first();
            var next = key == keys.UP ? current.prev() : current.next();
            if (next.length) {
                // skip the divider
                if (next.hasClass("divider")) {
                    next = key == keys.UP ? next.prev() : next.next();
                }
                this._highlightListItem(next);
                this._scrollTo(next);
            }
        },
        // select the currently highlighted item
        _handleEnterKey: function () {
            var currentCountry = this.countryList.children(".highlight").first();
            if (currentCountry.length) {
                this._selectListItem(currentCountry);
            }
        },
        _filterCountries: function (letters) {
            var countries = this.countryListItems.filter(function () {
                return $(this).text().toUpperCase().indexOf(letters) === 0 && !$(this).hasClass("preferred");
            });
            if (countries.length) {
                // if one is already highlighted, then we want the next one
                var highlightedCountry = countries.filter(".highlight").first(), listItem;
                if (highlightedCountry && highlightedCountry.next() && highlightedCountry.next().text().toUpperCase().indexOf(letters) === 0) {
                    listItem = highlightedCountry.next();
                } else {
                    listItem = countries.first();
                }
                // update highlighting and scroll
                this._highlightListItem(listItem);
                this._scrollTo(listItem);
            }
        },
        // Update the selected flag using the input's current value
        _updateFlagFromInputVal: function () {
            var that = this;
            // try and extract valid country from input
            var value = this.countryInput.val().replace(/(?=[() ])/g, '\\');
            if (value) {
                var countryCodes = [];
                var matcher = new RegExp(value, "i");
                // Check for ISO codes only
                if (value.length <= 2) {
                    for (var i = 0; i < this.countries.length; i++) {
                        if (this.countries[i].iso2.match(matcher)) {
                            countryCodes.push(this.countries[i].iso2);
                        }
                    }
                }
                // If no previous matches / larger than 2 chars, then search country name
                if (countryCodes.length == 0) {
                    for (var i = 0; i < this.countries.length; i++) {
                        if (this.countries[i].name.match(matcher)) {
                            countryCodes.push(this.countries[i].iso2);
                        }
                    }
                }
                // Check if one of the matching countries is already selected
                var alreadySelected = false;
                $.each(countryCodes, function (i, c) {
                    if (that.selectedFlagInner.hasClass(c)) {
                        alreadySelected = true;
                    }
                });
                if (!alreadySelected) {
                    this._selectFlag(countryCodes[0]);
                    this.countryCodeInput.val(countryCodes[0]).trigger("change");
                }
                // Matching country found
                return true;
            }
            // No match found
            return false;
        },
        // remove highlighting from other list items and highlight the given item
        _highlightListItem: function (listItem) {
            this.countryListItems.removeClass("highlight");
            listItem.addClass("highlight");
        },
        // find the country data for the given country code
        // the ignoreOnlyCountriesOption is only used during init() while parsing the onlyCountries array
        _getCountryData: function (countryCode, ignoreOnlyCountriesOption) {
            var countryList = ignoreOnlyCountriesOption ? allCountries : this.countries;
            for (var i = 0; i < countryList.length; i++) {
                if (countryList[i].iso2 == countryCode) {
                    return countryList[i];
                }
            }
            return null;
        },
        // update the selected flag and the active list item
        _selectFlag: function (countryCode) {
            if (!countryCode) {
                return false;
            }
            this.selectedFlagInner.attr("class", "flag " + countryCode);
            // update the title attribute
            var countryData = this._getCountryData(countryCode);
            this.selectedFlagInner.parent().attr("title", countryData.name);
            // update the active list item
            var listItem = this.countryListItems.children(".flag." + countryCode).first().parent();
            this.countryListItems.removeClass("active");
            listItem.addClass("active");
        },
        // called when the user selects a list item from the dropdown
        _selectListItem: function (listItem) {
            // update selected flag and active list item
            var countryCode = listItem.attr("data-country-code");
            this._selectFlag(countryCode);
            this._closeDropdown();
            // update input value
            this._updateName(countryCode);
            this.countryInput.trigger("change");
            this.countryCodeInput.trigger("change");
            // focus the input
            this._focus();
        },
        // close the dropdown and unbind any listeners
        _closeDropdown: function () {
            this.countryList.addClass("hide");
            // update the arrow
            this.selectedFlagInner.parent().children(".arrow").removeClass("up");
            // unbind event listeners
            $(document).off("keydown" + this.ns);
            $("html").off("click" + this.ns);
            // unbind both hover and click listeners
            this.countryList.off(this.ns);
            this.typedLetters = "";
        },
        // check if an element is visible within its container, else scroll until it is
        _scrollTo: function (element) {
            if (!element || !element.offset()) {
                return;
            }
            var container = this.countryList, containerHeight = container.height(), containerTop = container.offset().top, containerBottom = containerTop + containerHeight, elementHeight = element.outerHeight(), elementTop = element.offset().top, elementBottom = elementTop + elementHeight, newScrollTop = elementTop - containerTop + container.scrollTop();
            if (elementTop < containerTop) {
                // scroll up
                container.scrollTop(newScrollTop);
            } else if (elementBottom > containerBottom) {
                // scroll down
                var heightDifference = containerHeight - elementHeight;
                container.scrollTop(newScrollTop - heightDifference);
            }
        },
        // Replace any existing country name with the new one
        _updateName: function (countryCode) {
            this.countryCodeInput.val(countryCode).trigger("change");
            this.countryInput.val(this._getCountryData(countryCode).name);
            this.countryInput.attr('data-dialcode', this._getCountryData(countryCode).s);
        },
        /********************
         *  PUBLIC METHODS
         ********************/
        // this is called when the geoip call returns
        handleAutoCountry: function () {
            if (this.options.initialCountry === "auto") {
                // we must set this even if there is an initial val in the input: in case the initial val is invalid and they delete it - they should see their auto country
                this.defaultCountry = $.fn[pluginName].autoCountry;
                // if there's no initial value in the input, then update the flag
                if (!this.countryInput.val()) {
                    this.selectCountry(this.defaultCountry);
                }
                this.autoCountryDeferred.resolve();
            }
        },
        // get the country data for the currently selected flag
        getSelectedCountryData: function () {
            // rely on the fact that we only set 2 classes on the selected flag element:
            // the first is "flag" and the second is the 2-char country code
            var countryCode = this.selectedFlagInner.attr("class").split(" ")[1];
            return this._getCountryData(countryCode);
        },
        // update the selected flag
        selectCountry: function (countryCode) {
            countryCode = countryCode.toLowerCase();
            // check if already selected
            if (!this.selectedFlagInner.hasClass(countryCode)) {
                this._selectFlag(countryCode);
                this._updateName(countryCode);
            }
        },
        // set the input value and update the flag
        setCountry: function (country) {
            this.countryInput.val(country);
            this._updateFlagFromInputVal();
        },
        // remove plugin
        destroy: function () {
            // stop listeners
            this.countryInput.off(this.ns);
            this.selectedFlagInner.parent().off(this.ns);
            // remove markup
            var container = this.countryInput.parent();
            container.before(this.countryInput).remove();
        }
    };
    // adapted to allow public functions
    // using https://github.com/jquery-boilerplate/jquery-boilerplate/wiki/Extending-jQuery-Boilerplate
    $.fn[pluginName] = function (options) {
        var args = arguments;
        // Is the first parameter an object (options), or was omitted,
        // instantiate a new instance of the plugin.
        if (options === undefined || typeof options === "object") {
            return this.each(function () {
                if (!$.data(this, "plugin_" + pluginName)) {
                    $.data(this, "plugin_" + pluginName, new Plugin(this, options));
                }
            });
        } else if (typeof options === "string" && options[0] !== "_" && options !== "init") {
            // If the first parameter is a string and it doesn't start
            // with an underscore or "contains" the `init`-function,
            // treat this as a call to a public method.
            // Cache the method call to make it possible to return a value
            var returns;
            this.each(function () {
                var instance = $.data(this, "plugin_" + pluginName);
                // Tests that there's already a plugin-instance
                // and checks that the requested public method exists
                if (instance instanceof Plugin && typeof instance[options] === "function") {
                    // Call the method of our plugin instance,
                    // and pass it the supplied arguments.
                    returns = instance[options].apply(instance, Array.prototype.slice.call(args, 1));
                }
                // Allow instances to be destroyed via the 'destroy' method
                if (options === "destroy") {
                    $.data(this, "plugin_" + pluginName, null);
                }
            });
            // If the earlier cached method gives a value back return the value,
            // otherwise return this to preserve chainability.
            return returns !== undefined ? returns : this;
        }
    };
    /********************
     *  STATIC METHODS
     ********************/
    // get the country data object
    $.fn[pluginName].getCountryData = function () {
        return allCountries;
    };
    // set the country data object
    $.fn[pluginName].setCountryData = function (obj) {
        allCountries = obj;
    };
    // Tell JSHint to ignore this warning: "character may get silently deleted by one or more browsers"
    // jshint -W100
    // Array of country objects for the flag dropdown.
    // Each contains a name and country code (ISO 3166-1 alpha-2).
    //
    // Note: using single char property names to keep filesize down
    // n = name
    // i = iso2 (2-char country code)
    var allCountries = $.each( 
            [
                {
                    "s": "+93",
                    "i": "af",
                    "n": "Afghanistan"
                },
                {
                    "s": "+340",
                    "i": "ax",
                    "n": "Aland Islands"
                },
                {
                    "s": "+355",
                    "i": "al",
                    "n": "Albania"
                },
                {
                    "s": "+213",
                    "i": "dz",
                    "n": "Algeria"
                },
                {
                    "s": "+1-684",
                    "i": "as",
                    "n": "American Samoa"
                },
                {
                    "s": "+376",
                    "i": "ad",
                    "n": "Andorra"
                },
                {
                    "s": "+244",
                    "i": "ao",
                    "n": "Angola"
                },
                {
                    "s": "+1-264",
                    "i": "ai",
                    "n": "Anguilla"
                },
                {
                    "s": "+672",
                    "i": "aq",
                    "n": "Antarctica"
                },
                {
                    "s": "+1-268",
                    "i": "ag",
                    "n": "Antigua And Barbuda"
                },
                {
                    "s": "+54",
                    "i": "ar",
                    "n": "Argentina"
                },
                {
                    "s": "+374",
                    "i": "am",
                    "n": "Armenia"
                },
                {
                    "s": "+297",
                    "i": "aw",
                    "n": "Aruba"
                },
                {
                    "s": "+55",
                    "i": "br",
                    "n": "Brazil"
                },
                {
                    "s": "+246",
                    "i": "io",
                    "n": "British Indian Ocean Territory"
                },
                {
                    "s": "+673",
                    "i": "bn",
                    "n": "Brunei"
                },
                {
                    "s": "+359",
                    "i": "bg",
                    "n": "Bulgaria"
                },
                {
                    "s": "+226",
                    "i": "bf",
                    "n": "Burkina Faso"
                },
                {
                    "s": "+257",
                    "i": "bi",
                    "n": "Burundi"
                },
                {
                    "s": "+855",
                    "i": "kh",
                    "n": "Cambodia"
                },
                {
                    "s": "+237",
                    "i": "cm",
                    "n": "Cameroon"
                },
                {
                    "s": "+1",
                    "i": "ca",
                    "n": "Canada"
                },
                {
                    "s": "+238",
                    "i": "cv",
                    "n": "Cape Verde"
                },
                {
                    "s": "+1-345",
                    "i": "ky",
                    "n": "Cayman Islands"
                },
                {
                    "s": "+236",
                    "i": "cf",
                    "n": "Central African Republic"
                },
                {
                    "s": "+235",
                    "i": "td",
                    "n": "Chad"
                },
                {
                    "s": "+56",
                    "i": "cl",
                    "n": "Chile"
                },
                {
                    "s": "+86",
                    "i": "cn",
                    "n": "China"
                },
                {
                    "s": "+61",
                    "i": "cx",
                    "n": "Christmas Island"
                },
                {
                    "s": "+61",
                    "i": "cc",
                    "n": "Cocos (Keeling) Islands"
                },
                {
                    "s": "+57",
                    "i": "co",
                    "n": "Colombia"
                },
                {
                    "s": "+269",
                    "i": "km",
                    "n": "Comoros"
                },
                {
                    "s": "+242",
                    "i": "cg",
                    "n": "Congo"
                },
                {
                    "s": "+682",
                    "i": "ck",
                    "n": "Cook Islands"
                },
                {
                    "s": "+506",
                    "i": "cr",
                    "n": "Costa Rica"
                },
                {
                    "s": "+225",
                    "i": "ci",
                    "n": "Cote D'Ivoire (Ivory Coast)"
                },
                {
                    "s": "+385",
                    "i": "hr",
                    "n": "Croatia"
                },
                {
                    "s": "+53",
                    "i": "cu",
                    "n": "Cuba"
                },
                {
                    "s": "+599",
                    "i": "cw",
                    "n": "CuraÃ§ao"
                },
                {
                    "s": "+357",
                    "i": "cy",
                    "n": "Cyprus"
                },
                {
                    "s": "+420",
                    "i": "cz",
                    "n": "Czech Republic"
                },
                {
                    "s": "+243",
                    "i": "cd",
                    "n": "Democratic Republic of the Congo"
                },
                {
                    "s": "+45",
                    "i": "dk",
                    "n": "Denmark"
                },
                {
                    "s": "+253",
                    "i": "dj",
                    "n": "Djibouti"
                },
                {
                    "s": "+1-767",
                    "i": "dm",
                    "n": "Dominica"
                },
                {
                    "s": "+1-809",
                    "i": "do",
                    "n": "Dominican Republic"
                },
                {
                    "s": "+670",
                    "i": "tl",
                    "n": "East Timor"
                },
                {
                    "s": "+593",
                    "i": "ec",
                    "n": "Ecuador"
                },
                {
                    "s": "+20",
                    "i": "eg",
                    "n": "Egypt"
                },
                {
                    "s": "+503",
                    "i": "sv",
                    "n": "El Salvador"
                },
                {
                    "s": "+240",
                    "i": "gq",
                    "n": "Equatorial Guinea"
                },
                {
                    "s": "+61",
                    "i": "au",
                    "n": "Australia"
                },
                {
                    "s": "+43",
                    "i": "at",
                    "n": "Austria"
                },
                {
                    "s": "+994",
                    "i": "az",
                    "n": "Azerbaijan"
                },
                {
                    "s": "+973",
                    "i": "bh",
                    "n": "Bahrain"
                },
                {
                    "s": "+880",
                    "i": "bd",
                    "n": "Bangladesh"
                },
                {
                    "s": "+1-246",
                    "i": "bb",
                    "n": "Barbados"
                },
                {
                    "s": "+375",
                    "i": "by",
                    "n": "Belarus"
                },
                {
                    "s": "+32",
                    "i": "be",
                    "n": "Belgium"
                },
                {
                    "s": "+501",
                    "i": "bz",
                    "n": "Belize"
                },
                {
                    "s": "+229",
                    "i": "bj",
                    "n": "Benin"
                },
                {
                    "s": "+1-441",
                    "i": "bm",
                    "n": "Bermuda"
                },
                {
                    "s": "+975",
                    "i": "bt",
                    "n": "Bhutan"
                },
                {
                    "s": "+591",
                    "i": "bo",
                    "n": "Bolivia"
                },
                {
                    "s": "+599",
                    "i": "bq",
                    "n": "Bonaire, Sint Eustatius and Saba"
                },
                {
                    "s": "+387",
                    "i": "ba",
                    "n": "Bosnia and Herzegovina"
                },
                {
                    "s": "+267",
                    "i": "bw",
                    "n": "Botswana"
                },
                {
                    "s": "+55",
                    "i": "bv",
                    "n": "Bouvet Island"
                },
                {
                    "s": "+970",
                    "i": "ps",
                    "n": "Palestinian Territory Occupied"
                },
                {
                    "s": "+507",
                    "i": "pa",
                    "n": "Panama"
                },
                {
                    "s": "+675",
                    "i": "pg",
                    "n": "Papua new Guinea"
                },
                {
                    "s": "+595",
                    "i": "py",
                    "n": "Paraguay"
                },
                {
                    "s": "+51",
                    "i": "pe",
                    "n": "Peru"
                },
                {
                    "s": "+63",
                    "i": "ph",
                    "n": "Philippines"
                },
                {
                    "s": "+870",
                    "i": "pn",
                    "n": "Pitcairn Island"
                },
                {
                    "s": "+48",
                    "i": "pl",
                    "n": "Poland"
                },
                {
                    "s": "+351",
                    "i": "pt",
                    "n": "Portugal"
                },
                {
                    "s": "+1-787",
                    "i": "pr",
                    "n": "Puerto Rico"
                },
                {
                    "s": "+974",
                    "i": "qa",
                    "n": "Qatar"
                },
                {
                    "s": "+262",
                    "i": "re",
                    "n": "Reunion"
                },
                {
                    "s": "+40",
                    "i": "ro",
                    "n": "Romania"
                },
                {
                    "s": "+221",
                    "i": "sn",
                    "n": "Senegal"
                },
                {
                    "s": "+381",
                    "i": "rs",
                    "n": "Serbia"
                },
                {
                    "s": "+248",
                    "i": "sc",
                    "n": "Seychelles"
                },
                {
                    "s": "+232",
                    "i": "sl",
                    "n": "Sierra Leone"
                },
                {
                    "s": "+65",
                    "i": "sg",
                    "n": "Singapore"
                },
                {
                    "s": "+1721",
                    "i": "sx",
                    "n": "Sint Maarten (Dutch part)"
                },
                {
                    "s": "+421",
                    "i": "sk",
                    "n": "Slovakia"
                },
                {
                    "s": "+386",
                    "i": "si",
                    "n": "Slovenia"
                },
                {
                    "s": "+677",
                    "i": "sb",
                    "n": "Solomon Islands"
                },
                {
                    "s": "+252",
                    "i": "so",
                    "n": "Somalia"
                },
                {
                    "s": "+27",
                    "i": "za",
                    "n": "South Africa"
                },
                {
                    "s": "+500",
                    "i": "gs",
                    "n": "South Georgia"
                },
                {
                    "s": "+82",
                    "i": "kr",
                    "n": "South Korea"
                },
                {
                    "s": "+211",
                    "i": "ss",
                    "n": "South Sudan"
                },
                {
                    "s": "+34",
                    "i": "es",
                    "n": "Spain"
                },
                {
                    "s": "+94",
                    "i": "lk",
                    "n": "Sri Lanka"
                },
                {
                    "s": "+249",
                    "i": "sd",
                    "n": "Sudan"
                },
                {
                    "s": "+597",
                    "i": "sr",
                    "n": "Suriname"
                },
                {
                    "s": "+47",
                    "i": "sj",
                    "n": "Svalbard And Jan Mayen Islands"
                },
                {
                    "s": "+268",
                    "i": "sz",
                    "n": "Swaziland"
                },
                {
                    "s": "+46",
                    "i": "se",
                    "n": "Sweden"
                },
                {
                    "s": "+41",
                    "i": "ch",
                    "n": "Switzerland"
                },
                {
                    "s": "+7",
                    "i": "ru",
                    "n": "Russia"
                },
                {
                    "s": "+250",
                    "i": "rw",
                    "n": "Rwanda"
                },
                {
                    "s": "+290",
                    "i": "sh",
                    "n": "Saint Helena"
                },
                {
                    "s": "+1-869",
                    "i": "kn",
                    "n": "Saint Kitts And Nevis"
                },
                {
                    "s": "+1-758",
                    "i": "lc",
                    "n": "Saint Lucia"
                },
                {
                    "s": "+508",
                    "i": "pm",
                    "n": "Saint Pierre and Miquelon"
                },
                {
                    "s": "+1-784",
                    "i": "vc",
                    "n": "Saint Vincent And The Grenadines"
                },
                {
                    "s": "+590",
                    "i": "bl",
                    "n": "Saint-Barthelemy"
                },
                {
                    "s": "+590",
                    "i": "mf",
                    "n": "Saint-Martin (French part)"
                },
                {
                    "s": "+685",
                    "i": "ws",
                    "n": "Samoa"
                },
                {
                    "s": "+378",
                    "i": "sm",
                    "n": "San Marino"
                },
                {
                    "s": "+239",
                    "i": "st",
                    "n": "Sao Tome and Principe"
                },
                {
                    "s": "+966",
                    "i": "sa",
                    "n": "Saudi Arabia"
                },
                {
                    "s": "+291",
                    "i": "er",
                    "n": "Eritrea"
                },
                {
                    "s": "+372",
                    "i": "ee",
                    "n": "Estonia"
                },
                {
                    "s": "+251",
                    "i": "et",
                    "n": "Ethiopia"
                },
                {
                    "s": "+500",
                    "i": "fk",
                    "n": "Falkland Islands"
                },
                {
                    "s": "+298",
                    "i": "fo",
                    "n": "Faroe Islands"
                },
                {
                    "s": "+679",
                    "i": "fj",
                    "n": "Fiji Islands"
                },
                {
                    "s": "+358",
                    "i": "fi",
                    "n": "Finland"
                },
                {
                    "s": "+33",
                    "i": "fr",
                    "n": "France"
                },
                {
                    "s": "+594",
                    "i": "gf",
                    "n": "French Guiana"
                },
                {
                    "s": "+689",
                    "i": "pf",
                    "n": "French Polynesia"
                },
                {
                    "s": "+262",
                    "i": "tf",
                    "n": "French Southern Territories"
                },
                {
                    "s": "+241",
                    "i": "ga",
                    "n": "Gabon"
                },
                {
                    "s": "+220",
                    "i": "gm",
                    "n": "Gambia The"
                },
                {
                    "s": "+995",
                    "i": "ge",
                    "n": "Georgia"
                },
                {
                    "s": "+49",
                    "i": "de",
                    "n": "Germany"
                },
                {
                    "s": "+233",
                    "i": "gh",
                    "n": "Ghana"
                },
                {
                    "s": "+350",
                    "i": "gi",
                    "n": "Gibraltar"
                },
                {
                    "s": "+30",
                    "i": "gr",
                    "n": "Greece"
                },
                {
                    "s": "+299",
                    "i": "gl",
                    "n": "Greenland"
                },
                {
                    "s": "+1-473",
                    "i": "gd",
                    "n": "Grenada"
                },
                {
                    "s": "+590",
                    "i": "gp",
                    "n": "Guadeloupe"
                },
                {
                    "s": "+1-671",
                    "i": "gu",
                    "n": "Guam"
                },
                {
                    "s": "+502",
                    "i": "gt",
                    "n": "Guatemala"
                },
                {
                    "s": "+44-1481",
                    "i": "gg",
                    "n": "Guernsey and Alderney"
                },
                {
                    "s": "+224",
                    "i": "gn",
                    "n": "Guinea"
                },
                {
                    "s": "+245",
                    "i": "gw",
                    "n": "Guinea-Bissau"
                },
                {
                    "s": "+592",
                    "i": "gy",
                    "n": "Guyana"
                },
                {
                    "s": "+509",
                    "i": "ht",
                    "n": "Haiti"
                },
                {
                    "s": "+672",
                    "i": "hm",
                    "n": "Heard Island and McDonald Islands"
                },
                {
                    "s": "+504",
                    "i": "hn",
                    "n": "Honduras"
                },
                {
                    "s": "+852",
                    "i": "hk",
                    "n": "Hong Kong S.A.R."
                },
                {
                    "s": "+36",
                    "i": "hu",
                    "n": "Hungary"
                },
                {
                    "s": "+354",
                    "i": "is",
                    "n": "Iceland"
                },
                {
                    "s": "+91",
                    "i": "in",
                    "n": "India"
                },
                {
                    "s": "+62",
                    "i": "id",
                    "n": "Indonesia"
                },
                {
                    "s": "+98",
                    "i": "ir",
                    "n": "Iran"
                },
                {
                    "s": "+964",
                    "i": "iq",
                    "n": "Iraq"
                },
                {
                    "s": "+353",
                    "i": "ie",
                    "n": "Ireland"
                },
                {
                    "s": "+972",
                    "i": "il",
                    "n": "Israel"
                },
                {
                    "s": "+39",
                    "i": "it",
                    "n": "Italy"
                },
                {
                    "s": "+1-876",
                    "i": "jm",
                    "n": "Jamaica"
                },
                {
                    "s": "+81",
                    "i": "jp",
                    "n": "Japan"
                },
                {
                    "s": "+44-1534",
                    "i": "je",
                    "n": "Jersey"
                },
                {
                    "s": "+962",
                    "i": "jo",
                    "n": "Jordan"
                },
                {
                    "s": "+7",
                    "i": "kz",
                    "n": "Kazakhstan"
                },
                {
                    "s": "+254",
                    "i": "ke",
                    "n": "Kenya"
                },
                {
                    "s": "+686",
                    "i": "ki",
                    "n": "Kiribati"
                },
                {
                    "s": "+383",
                    "i": "xk",
                    "n": "Kosovo"
                },
                {
                    "s": "+965",
                    "i": "kw",
                    "n": "Kuwait"
                },
                {
                    "s": "+996",
                    "i": "kg",
                    "n": "Kyrgyzstan"
                },
                {
                    "s": "+856",
                    "i": "la",
                    "n": "Laos"
                },
                {
                    "s": "+371",
                    "i": "lv",
                    "n": "Latvia"
                },
                {
                    "s": "+961",
                    "i": "lb",
                    "n": "Lebanon"
                },
                {
                    "s": "+266",
                    "i": "ls",
                    "n": "Lesotho"
                },
                {
                    "s": "+231",
                    "i": "lr",
                    "n": "Liberia"
                },
                {
                    "s": "+218",
                    "i": "ly",
                    "n": "Libya"
                },
                {
                    "s": "+423",
                    "i": "li",
                    "n": "Liechtenstein"
                },
                {
                    "s": "+370",
                    "i": "lt",
                    "n": "Lithuania"
                },
                {
                    "s": "+352",
                    "i": "lu",
                    "n": "Luxembourg"
                },
                {
                    "s": "+853",
                    "i": "mo",
                    "n": "Macau S.A.R."
                },
                {
                    "s": "+261",
                    "i": "mg",
                    "n": "Madagascar"
                },
                {
                    "s": "+265",
                    "i": "mw",
                    "n": "Malawi"
                },
                {
                    "s": "+60",
                    "i": "my",
                    "n": "Malaysia"
                },
                {
                    "s": "+960",
                    "i": "mv",
                    "n": "Maldives"
                },
                {
                    "s": "+223",
                    "i": "ml",
                    "n": "Mali"
                },
                {
                    "s": "+356",
                    "i": "mt",
                    "n": "Malta"
                },
                {
                    "s": "+44-1624",
                    "i": "im",
                    "n": "Man (Isle of)"
                },
                {
                    "s": "+692",
                    "i": "mh",
                    "n": "Marshall Islands"
                },
                {
                    "s": "+596",
                    "i": "mq",
                    "n": "Martinique"
                },
                {
                    "s": "+222",
                    "i": "mr",
                    "n": "Mauritania"
                },
                {
                    "s": "+230",
                    "i": "mu",
                    "n": "Mauritius"
                },
                {
                    "s": "+262",
                    "i": "yt",
                    "n": "Mayotte"
                },
                {
                    "s": "+52",
                    "i": "mx",
                    "n": "Mexico"
                },
                {
                    "s": "+691",
                    "i": "fm",
                    "n": "Micronesia"
                },
                {
                    "s": "+373",
                    "i": "md",
                    "n": "Moldova"
                },
                {
                    "s": "+377",
                    "i": "mc",
                    "n": "Monaco"
                },
                {
                    "s": "+976",
                    "i": "mn",
                    "n": "Mongolia"
                },
                {
                    "s": "+382",
                    "i": "me",
                    "n": "Montenegro"
                },
                {
                    "s": "+1-664",
                    "i": "ms",
                    "n": "Montserrat"
                },
                {
                    "s": "+212",
                    "i": "ma",
                    "n": "Morocco"
                },
                {
                    "s": "+258",
                    "i": "mz",
                    "n": "Mozambique"
                },
                {
                    "s": "+95",
                    "i": "mm",
                    "n": "Myanmar"
                },
                {
                    "s": "+264",
                    "i": "na",
                    "n": "Namibia"
                },
                {
                    "s": "+674",
                    "i": "nr",
                    "n": "Nauru"
                },
                {
                    "s": "+977",
                    "i": "np",
                    "n": "Nepal"
                },
                {
                    "s": "+31",
                    "i": "nl",
                    "n": "Netherlands"
                },
                {
                    "s": "+687",
                    "i": "nc",
                    "n": "New Caledonia"
                },
                {
                    "s": "+64",
                    "i": "nz",
                    "n": "New Zealand"
                },
                {
                    "s": "+505",
                    "i": "ni",
                    "n": "Nicaragua"
                },
                {
                    "s": "+227",
                    "i": "ne",
                    "n": "Niger"
                },
                {
                    "s": "+234",
                    "i": "ng",
                    "n": "Nigeria"
                },
                {
                    "s": "+683",
                    "i": "nu",
                    "n": "Niue"
                },
                {
                    "s": "+672",
                    "i": "nf",
                    "n": "Norfolk Island"
                },
                {
                    "s": "+850",
                    "i": "kp",
                    "n": "North Korea"
                },
                {
                    "s": "+389",
                    "i": "mk",
                    "n": "North Macedonia"
                },
                {
                    "s": "+1-670",
                    "i": "mp",
                    "n": "Northern Mariana Islands"
                },
                {
                    "s": "+47",
                    "i": "no",
                    "n": "Norway"
                },
                {
                    "s": "+968",
                    "i": "om",
                    "n": "Oman"
                },
                {
                    "s": "+92",
                    "i": "pk",
                    "n": "Pakistan"
                },
                {
                    "s": "+680",
                    "i": "pw",
                    "n": "Palau"
                },
                {
                    "s": "+963",
                    "i": "sy",
                    "n": "Syria"
                },
                {
                    "s": "+886",
                    "i": "tw",
                    "n": "Taiwan"
                },
                {
                    "s": "+992",
                    "i": "tj",
                    "n": "Tajikistan"
                },
                {
                    "s": "+255",
                    "i": "tz",
                    "n": "Tanzania"
                },
                {
                    "s": "+66",
                    "i": "th",
                    "n": "Thailand"
                },
                {
                    "s": "+1-242",
                    "i": "bs",
                    "n": "The Bahamas"
                },
                {
                    "s": "+228",
                    "i": "tg",
                    "n": "Togo"
                },
                {
                    "s": "+690",
                    "i": "tk",
                    "n": "Tokelau"
                },
                {
                    "s": "+676",
                    "i": "to",
                    "n": "Tonga"
                },
                {
                    "s": "+1-868",
                    "i": "tt",
                    "n": "Trinidad And Tobago"
                },
                {
                    "s": "+216",
                    "i": "tn",
                    "n": "Tunisia"
                },
                {
                    "s": "+90",
                    "i": "tr",
                    "n": "Turkey"
                },
                {
                    "s": "+993",
                    "i": "tm",
                    "n": "Turkmenistan"
                },
                {
                    "s": "+1-649",
                    "i": "tc",
                    "n": "Turks And Caicos Islands"
                },
                {
                    "s": "+688",
                    "i": "tv",
                    "n": "Tuvalu"
                },
                {
                    "s": "+256",
                    "i": "ug",
                    "n": "Uganda"
                },
                {
                    "s": "+380",
                    "i": "ua",
                    "n": "Ukraine"
                },
                {
                    "s": "+971",
                    "i": "ae",
                    "n": "United Arab Emirates"
                },
                {
                    "s": "+44",
                    "i": "gb",
                    "n": "United Kingdom"
                },
                {
                    "s": "+681",
                    "i": "wf",
                    "n": "Wallis And Futuna Islands"
                },
                {
                    "s": "+212",
                    "i": "eh",
                    "n": "Western Sahara"
                },
                {
                    "s": "+967",
                    "i": "ye",
                    "n": "Yemen"
                },
                {
                    "s": "+1",
                    "i": "us",
                    "n": "United States"
                },
                {
                    "s": "+1",
                    "i": "um",
                    "n": "United States Minor Outlying Islands"
                },
                {
                    "s": "+598",
                    "i": "uy",
                    "n": "Uruguay"
                },
                {
                    "s": "+998",
                    "i": "uz",
                    "n": "Uzbekistan"
                },
                {
                    "s": "+678",
                    "i": "vu",
                    "n": "Vanuatu"
                },
                {
                    "s": "+379",
                    "i": "va",
                    "n": "Vatican City State (Holy See)"
                },
                {
                    "s": "+58",
                    "i": "ve",
                    "n": "Venezuela"
                },
                {
                    "s": "+84",
                    "i": "vn",
                    "n": "Vietnam"
                },
                {
                    "s": "+1-284",
                    "i": "vg",
                    "n": "Virgin Islands (British)"
                },
                {
                    "s": "+1-340",
                    "i": "vi",
                    "n": "Virgin Islands (US)"
                },
                {
                    "s": "+260",
                    "i": "zm",
                    "n": "Zambia"
                },
                {
                    "s": "+263",
                    "i": "zw",
                    "n": "Zimbabwe"
                },
                {
                    "s": "",
                    "i": "defult_flag",
                    "n": ""
                }
            ], function (i, c) {
        c.name = c.n;
        c.iso2 = c.i;
        c.stdC = c.s;
        delete c.n;
        delete c.i;
    });
});
