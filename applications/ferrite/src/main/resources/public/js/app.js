/*
 * ferrite — the small amount of client behaviour the server cannot express.
 *
 * This file exists as an ASSET and not as an inline <script> because the Content-Security-Policy in
 * application.conf has no 'unsafe-inline' in script-src. That is a deliberate constraint, not an accident: an
 * observatory renders event payloads that arrived from devices, and the one thing it must never do is give a
 * producer a way to run script in an operator's browser.
 *
 * The division of labour follows ADR section 8.4: if answering a question requires the database it is htmx; if it is
 * purely presentational it is Alpine. Nothing here fetches, renders a list, or holds form state the server owns.
 */
(function () {
  'use strict';

  var RESULTS = 'results';
  var HEADING = 'results-heading';
  var SEARCH = 'q';

  function results() {
    return document.getElementById(RESULTS);
  }

  /*
   * aria-busy is a STATE, announced by assistive technology; a spinner is a picture, announced by nothing. Both are
   * driven from the same two events so they cannot disagree about whether a search is in flight.
   */
  document.body.addEventListener('htmx:beforeRequest', function () {
    var region = results();
    if (region) {
      region.setAttribute('aria-busy', 'true');
    }
  });

  document.body.addEventListener('htmx:afterRequest', function () {
    var region = results();
    if (region) {
      region.setAttribute('aria-busy', 'false');
    }
  });

  /*
   * Focus management after a swap.
   *
   * Without this, a screen-reader or keyboard user loses their place on every filter change: the element they were
   * on has just been removed from the document, so focus falls back to <body> and the next Tab starts from the top
   * of the page. Moving focus to the results heading (which is tabindex="-1") puts them at the start of what
   * changed. Paging is excluded on purpose — appending rows below is not a context change, and yanking focus back
   * to the heading mid-scroll would be worse than doing nothing.
   */
  document.body.addEventListener('htmx:afterSwap', function (event) {
    var target = event.detail && event.detail.target;
    if (!target || target.id !== RESULTS) {
      return;
    }
    var heading = document.getElementById(HEADING);
    if (heading) {
      heading.focus();
    }
  });

  /*
   * Keyboard navigation. `/` focuses the search box, j/k walk the result rows, Enter opens the focused row, Escape
   * leaves the current control. Every one of these targets a real <a> or <button>, so the same actions are reachable
   * by Tab alone and this is an accelerator rather than the only way in.
   */
  function rowLinks() {
    return Array.prototype.slice.call(document.querySelectorAll('#event-rows .row-open'));
  }

  function isTypingTarget(element) {
    if (!element) {
      return false;
    }
    var tag = element.tagName;
    return tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT' || element.isContentEditable;
  }

  function move(offset) {
    var links = rowLinks();
    if (links.length === 0) {
      return;
    }
    var current = links.indexOf(document.activeElement);
    var next = current < 0 ? 0 : Math.min(links.length - 1, Math.max(0, current + offset));
    links[next].focus();
  }

  document.addEventListener('keydown', function (event) {
    if (event.metaKey || event.ctrlKey || event.altKey) {
      return;
    }
    if (event.key === 'Escape') {
      if (document.activeElement && document.activeElement.blur) {
        document.activeElement.blur();
      }
      return;
    }
    if (isTypingTarget(event.target)) {
      return;
    }
    if (event.key === '/') {
      var search = document.getElementById(SEARCH);
      if (search) {
        event.preventDefault();
        search.focus();
        search.select();
      }
    } else if (event.key === 'j') {
      event.preventDefault();
      move(1);
    } else if (event.key === 'k') {
      event.preventDefault();
      move(-1);
    }
  });

  /*
   * Alpine's whole remit in this application: clipboard feedback and a persisted density toggle. Alpine 3 picks up
   * DOM that htmx swaps in through its own MutationObserver, so swapped fragments need no re-initialisation.
   */
  document.addEventListener('alpine:init', function () {
    window.Alpine.data('observatory', function () {
      return {
        copied: false,
        density: window.localStorage.getItem('ferrite.density') || 'comfortable',
        init: function () {
          document.body.dataset.density = this.density;
        },
        toggleDensity: function () {
          this.density = this.density === 'compact' ? 'comfortable' : 'compact';
          window.localStorage.setItem('ferrite.density', this.density);
          document.body.dataset.density = this.density;
        },
        copy: function (text) {
          var self = this;
          navigator.clipboard.writeText(text).then(function () {
            self.copied = true;
            window.setTimeout(function () {
              self.copied = false;
            }, 1500);
          });
        }
      };
    });
  });
})();
