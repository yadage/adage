import custombackend

custombackend.create_state()

x = custombackend.BACKENDDATA.get()

for identifier,proxy in x['proxies'].iteritems():
    if x['proxystate'][identifier] == 'CREATED':
        newresult = {'hello':'this is result number: {}'.format(identifier)}
        x['results'][identifier]  = newresult
        x['proxystate'][identifier]  = 'SUCCESS'
        custombackend.BACKENDDATA.commit(x)
        print 'commited result....'