REBAR = ./rebar
DIALYZER = dialyzer

DIALYZER_WARNINGS = -Wunmatched_returns -Werror_handling \
                    -Wrace_conditions -Wunderspecs

.PHONY: all compile test clean

all: compile

compile:
	@$(REBAR) compile

test: compile
	@$(REBAR) eunit

clean:
	@$(REBAR) clean

build-plt:
	@$(DIALYZER) --build_plt --output_plt .dialyzer_plt \
	    --apps kernel stdlib

dialyze: compile
	@$(DIALYZER) --src src --plt .dialyzer_plt $(DIALYZER_WARNINGS) | \
	    fgrep -vf .dialyzer-ignore-warnings
