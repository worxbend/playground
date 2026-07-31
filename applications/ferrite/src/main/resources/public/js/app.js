/*
 * ferrite — the small amount of client behaviour the server cannot express.
 *
 * This file exists as an ASSET and not as an inline <script> because the Content-Security-Policy in
 * application.conf has no 'unsafe-inline' in script-src. That is a deliberate constraint, not an accident: an
 * observatory renders event payloads that arrived from devices, and the one thing it must never do is give a
 * producer a way to run script in an operator's browser.
 *
 * The division of labour follows ADR section 8.4: if answering a question requires the database it is htmx; if it is
 * purely presentational it is Alpine. Nothing Alpine does here fetches, renders a list, or holds form state the
 * server owns.
 *
 * THE ONE EXCEPTION, STATED PLAINLY: the live tail. It needs an EventSource, which htmx can only drive through its
 * `sse` extension — a WebJar this build does not depend on, and adding a dependency is a build change. So the tail is
 * the `Tail` object below: plain JavaScript that opens the stream and inserts markup THE SERVER RENDERED. It never
 * templates a value out of an event, so device-supplied strings are still escaped by Twirl exactly once, in the same
 * template the search page uses. Alpine's part is the button label and the status text, which is presentation.
 */
(function () {
  'use strict';

  var RESULTS = 'results';
  var HEADING = 'results-heading';
  var SEARCH = 'q';
  var ROWS = 'event-rows';

  /* Rows kept in the DOM while tailing. A feed left open overnight is otherwise an unbounded list, and the tab dies
     of a slow leak that looks like "the browser is bad at tables". */
  var MAX_TAIL_ROWS = 500;

  function results() {
    return document.getElementById(RESULTS);
  }

  function rowContainer() {
    return document.getElementById(ROWS);
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
    /* The swapped-in list belongs to a different search than the open stream does, and the rows already arriving
       would be filed under a filter that is no longer on screen. Stopping is the only honest option.
       Guarded, because a tail that was never running has nothing to say about a filter change and a status line
       that announced one after every keystroke would be noise. */
    if (Tail.running()) {
      Tail.stop('Filter changed — press Go live to follow the new search.');
    }
    var heading = document.getElementById(HEADING);
    if (heading) {
      heading.focus();
    }
  });

  /* ------------------------------------------------------------------ the live tail */

  var Tail = {
    source: null,

    /* Set by the Alpine component so the button and the status line reflect what the connection is actually doing.
       Presentation lives there; the connection lives here. */
    onState: function () {},

    running: function () {
      return this.source !== null;
    },

    note: function (text, live) {
      this.onState(live, text);
    },

    /*
     * Opens the stream for whatever filter the address bar currently holds.
     *
     * `location.search` and not a URL rendered into the page: htmx pushes a new URL on every filter change, so the
     * address bar is the only source that is still correct for a results region the server rendered under a
     * different one.
     *
     * The cursor is the newest row on screen. Starting from it means the tail picks up exactly where the rendered
     * page stops — no gap, and no re-sending of rows the user is already looking at.
     */
    start: function (path) {
      var body = rowContainer();
      if (!body) {
        this.note('Nothing to follow yet — run a search that returns rows.', false);
        return;
      }
      if (body.getAttribute('data-order') !== 'newest') {
        this.note('Live tail follows newest-first order. Sort by newest to use it.', false);
        return;
      }

      var params = new URLSearchParams(window.location.search);
      params.delete('cursor');
      var first = body.querySelector('tr.event-row');
      if (first) {
        params.set('after', first.getAttribute('data-at'));
        params.set('afterUid', first.getAttribute('data-uid'));
      }

      var stream = new EventSource(path + '?' + params.toString());
      var self = this;

      stream.addEventListener('row', function (event) {
        self.insert(event.lastEventId, event.data);
      });

      stream.addEventListener('heartbeat', function () {
        self.note('Live', true);
      });

      stream.onerror = function () {
        /* readyState 2 is CLOSED: the browser has given up, which is what a 503 from the capacity guard looks like
           from here. Anything else is its own reconnect, which needs no help from us. */
        if (stream.readyState === 2) {
          self.stop('Disconnected. The server may be serving as many live tails as it will.');
        } else {
          self.note('Reconnecting…', true);
        }
      };

      this.source = stream;
      this.note('Live', true);
    },

    stop: function (reason) {
      if (this.source) {
        this.source.close();
        this.source = null;
      }
      this.note(reason || 'Paused', false);
    },

    /*
     * Prepends one server-rendered row.
     *
     * De-duplicated by uid because a resumed tail can legitimately be handed a position it has already passed — and
     * because a duplicated row in a feed is the kind of wrongness that makes an operator stop trusting the whole
     * page.
     */
    insert: function (uid, html) {
      var body = rowContainer();
      if (!body || !html) {
        return;
      }
      if (uid && body.querySelector('tr[data-uid="' + uid + '"]')) {
        return;
      }
      body.insertAdjacentHTML('afterbegin', html);
      var rows = body.querySelectorAll('tr.event-row');
      for (var i = MAX_TAIL_ROWS; i < rows.length; i++) {
        rows[i].remove();
      }
    }
  };

  /* ------------------------------------------------------------------ keyboard navigation */

  function rowLinks() {
    return Array.prototype.slice.call(document.querySelectorAll('#' + ROWS + ' .row-open'));
  }

  function isTypingTarget(element) {
    if (!element) {
      return false;
    }
    var tag = element.tagName;
    return tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT' || element.isContentEditable;
  }

  function markSelected(link) {
    document.querySelectorAll('#' + ROWS + ' tr.is-selected').forEach(function (row) {
      row.classList.remove('is-selected');
    });
    if (link) {
      var row = link.closest('tr');
      if (row) {
        row.classList.add('is-selected');
      }
    }
  }

  /*
   * Selection IS focus. Keeping a separate "current row" index would let the two disagree — the ring on one row and
   * Enter opening another — and the disagreement only shows up once someone mixes Tab with j/k.
   */
  function move(offset) {
    var links = rowLinks();
    if (links.length === 0) {
      return;
    }
    var current = links.indexOf(document.activeElement);
    var next = current < 0 ? 0 : Math.min(links.length - 1, Math.max(0, current + offset));
    links[next].focus();
    markSelected(links[next]);
  }

  function selectedLink() {
    var links = rowLinks();
    var index = links.indexOf(document.activeElement);
    return index < 0 ? null : links[index];
  }

  /*
   * Alpine's remit: the copy-to-clipboard feedback, the persisted density toggle, the live tail's button state, and
   * ONE keyboard handler (ADR section 8.4). Alpine 3 picks up DOM that htmx swaps in through its own
   * MutationObserver, so swapped fragments need no re-initialisation.
   */
  document.addEventListener('alpine:init', function () {
    window.Alpine.data('observatory', function () {
      return {
        copied: false,
        live: false,
        tailNote: 'Paused',
        density: window.localStorage.getItem('ferrite.density') || 'comfortable',

        init: function () {
          document.body.dataset.density = this.density;
          var self = this;
          Tail.onState = function (live, note) {
            self.live = live;
            self.tailNote = note;
          };
        },

        toggleTail: function () {
          if (Tail.running()) {
            Tail.stop('Paused');
            return;
          }
          var bar = document.querySelector('.tail-bar');
          Tail.start(bar ? bar.getAttribute('data-stream') : '/live');
        },

        /*
         * The single keyboard handler, bound once with x-on:keydown.window on <body>.
         *
         *   /      focus the search box
         *   j / k  move the row selection, which is the focus ring
         *   Enter  open the selected event
         *   Esc    close what is open: leave the control you are in and drop the selection
         *
         * Every one of these targets a real <a> or <button>, so the same actions are reachable by Tab alone and this
         * is an accelerator rather than the only way in. Modified keys are left to the browser: hijacking Ctrl-J or
         * Cmd-K would take a shortcut away from the user's own environment.
         */
        onKey: function (event) {
          if (event.metaKey || event.ctrlKey || event.altKey) {
            return;
          }
          if (event.key === 'Escape') {
            if (document.activeElement && document.activeElement.blur) {
              document.activeElement.blur();
            }
            markSelected(null);
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
          } else if (event.key === 'Enter') {
            var link = selectedLink();
            if (link) {
              /* preventDefault first: without it the browser also activates the focused link and the navigation
                 happens twice. */
              event.preventDefault();
              link.click();
            }
          }
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
