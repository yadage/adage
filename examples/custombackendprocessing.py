import custombackend

custombackend.create_state()

x = custombackend.BACKENDDATA.get()

def decide(task_id):
    print 'task {} '
    shall = raw_input("Shall we? (y/N) ").lower() == 'y'
    if shall:
        print 'ok we will extend.'
    return shall

for identifier,proxy in x['proxies'].iteritems():
    if x['proxystate'][identifier] == 'CREATED':
        print 
        newresult = {'hello':'this is result number: {}'.format(identifier)}
        x['results'][identifier]  = newresult
        x['proxystate'][identifier]  = 'SUCCESS'
        custombackend.BACKENDDATA.commit(x)
        print 'commited result....'