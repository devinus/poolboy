.PHONY: all compile clean

all: compile

compile:
	@./rebar compile

clean:
	@./rebar clean
