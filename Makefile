REBAR = ./rebar
DIALYZER = dialyzer

DIALYZER_WARNINGS = -Wunmatched_returns -Werror_handling \
                    -Wrace_conditions -Wunderspecs

.PHONY: all compile test clean get-deps build-plt dialyze

all: compile

compile:
	@$(REBAR) compile

test: compile
	@$(REBAR) eunit skip_deps=true

clean:
	@$(REBAR) clean

get-deps:
	@$(REBAR) get-deps

build-plt:
	@$(DIALYZER) --build_plt --output_plt .dialyzer_plt \
	    --apps kernel stdlib

dialyze: compile
	@$(DIALYZER) --src src --plt .dialyzer_plt $(DIALYZER_WARNINGS) | \
	    fgrep -vf .dialyzer-ignore-warnings
