REBAR = $(shell command -v rebar3 || echo ./rebar3)

.PHONY: all compile test qc clean

all: compile

compile:
	@$(REBAR) compile

test:
	@$(REBAR) eunit

qc: compile
	@$(REBAR) eqc

clean:
	@$(REBAR) clean

dialyze:
	@$(REBAR) dialyzer
