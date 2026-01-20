def ask_llm(foo):
    print(foo)

def any_test_fails():
    return True

def too_slow():
    return True

while any_test_fails():
    ask_llm("Make database")

while too_slow():
    ask_llm("Make database faster")