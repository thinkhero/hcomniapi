import json
from sqltools import *
from blockchain_utils import *
from property_service import getpropertyraw
from rpcclient import *
def get_balancedata_db_ROWS(addr):
  return dbSelect("""select
                       f1.propertyid, sp.propertytype, f1.balanceavailable, f1.pendingpos, f1.pendingneg, f1.balancereserved, f1.balancefrozen
                     from
                       (select
                          COALESCE(s1.propertyid,s2.propertyid) as propertyid, COALESCE(s1.balanceavailable,0) as balanceavailable, COALESCE(s1.balancefrozen,0) as balancefrozen,
                          COALESCE(s2.pendingpos,0) as pendingpos,COALESCE(s2.pendingneg,0) as pendingneg, COALESCE(s1.balancereserved,0) as balancereserved
                        from
                          (select propertyid,balanceavailable,balancereserved,balancefrozen
                           from addressbalances
                           where address=%s) s1
                        full join
                          (SELECT atx.propertyid,
                             sum(CASE WHEN atx.balanceavailablecreditdebit > 0 THEN atx.balanceavailablecreditdebit ELSE 0 END) AS pendingpos,
                             sum(CASE WHEN atx.balanceavailablecreditdebit < 0 THEN atx.balanceavailablecreditdebit ELSE 0 END) AS pendingneg
                           from
                             addressesintxs atx, transactions tx
                           where
                             atx.txdbserialnum=tx.txdbserialnum
                             and tx.txstate='pending'
                             and tx.txdbserialnum<-1
                             and atx.address=%s
                           group by
                             atx.propertyid) s2
                        on s1.propertyid=s2.propertyid) f1
                     inner join smartproperties sp
                     on f1.propertyid=sp.propertyid and (sp.protocol='Omni' or sp.protocol='Bitcoin')
                     order by f1.propertyid""",(addr,addr))
def get_balancedata_rpc_ROWS(addr):
  print "---------------------"
  print getallbalancesforaddress(addr)
  print "------------------------"
  return getallbalancesforaddress(addr)

def get_balancedata1(address):
    print ">>> enter into get_balancedata",address
    addr = re.sub(r'\W+', '', address) #check alphanumeric
    ROWS = get_balancedata_db_ROWS(address)
    #ROWS = get_balancedata_rpc_ROWS(address)
    print ">>>>>>>>>>>>>>>>>>>>>>"    
    print ROWS
    balance_data = { 'balance': [] }
    ret = bc_getbalance(addr)
    out = ret['bal']
    err = ret['error']
    for balrow in ROWS:
        cID = str(int(balrow[0])) #currency id
        sym_t = ('BTC' if cID == '0' else ('OMNI' if cID == '1' else ('T-OMNI' if cID == '2' else 'SP' + cID) ) ) #symbol template
        #1 = new indivisible property, 2=new divisible property (per spec)
        divi = True if int(balrow[1]) == 2 else False
        res = { 'symbol' : sym_t, 'divisible' : divi, 'id' : cID }
        #inject property details but remove issuanecs
        res['propertyinfo'] = getpropertyraw(cID)
        if 'issuances' in res['propertyinfo']:
          res['propertyinfo'].pop('issuances')
        res['pendingpos'] = str(long(balrow[3]))
        res['pendingneg'] = str(long(balrow[4]))
        res['reserved'] = str(long(balrow[5]))
        res['frozen'] = str(long(balrow[6]))
        if cID == '0':
          #get btc balance from bc api's
          if err != None or out == '':
            #btc_balance[ 'value' ] = str(long(-555))
            btc_balance[ 'value' ] = str(long(0))
            btc_balance[ 'error' ] = True
          else:
            try:
              if balrow[4] < 0:
                #res['value'] = str(long( json.loads( out )[0][ 'paid' ]) + str(long(balrow[4]))
                #res['value'] = str(long( json.loads( out )['data']['balance']*1e8) + str(long(balrow[4]))
                res['value'] = str(long( out ) + long(balrow[4]))
              else:
                #res['value'] = str(long( json.loads( out )[0][ 'paid' ]))
                #res['value'] = str(long( json.loads( out )['data']['balance']*1e8))
                res['value'] = str(long( out ))
            except ValueError:
              #btc_balance[ 'value' ] = str(long(-555))
              btc_balance[ 'value' ] = str(long(0))
              btc_balance[ 'error' ] = True
        else:
          #get regular balance from db 
          if balrow[4] < 0 and not balrow[6] > 0:
            #update the 'available' balance immediately when the sender sent something. prevent double spend as long as its not frozen
            res['value'] = str(long(balrow[2]+balrow[4]))
          else:
            res['value'] = str(long(balrow[2]))

        #res['reserved_balance'] = ('%.8f' % float(balrow[5])).rstrip('0').rstrip('.')
        balance_data['balance'].append(res)

    #check if we got BTC data from DB, if not trigger manually add
    addbtc=True
    for x in balance_data['balance']:
      if "BTC" in x['symbol']:
        addbtc=False

    if addbtc:
      btc_balance = { 'symbol': 'BTC', 'divisible': True, 'id' : '0', 'error' : False }
      if err != None or out == '':
        #btc_balance[ 'value' ] = str(long(-555))
        btc_balance[ 'value' ] = str(long(0))
        btc_balance[ 'error' ] = True
      else:
        try:
          #btc_balance[ 'value' ] = str(long( json.loads( out )[0][ 'paid' ]))
          #btc_balance[ 'value' ] = str(long( json.loads( out )['data']['balance']*1e8 ))
          btc_balance[ 'value' ] = str(long( out ))
        except ValueError:
          #btc_balance[ 'value' ] = str(long(-555))
          btc_balance[ 'value' ] = str(long(0))
          btc_balance[ 'error' ] = True
      btc_balance['pendingpos'] = str(long(0))
      btc_balance['pendingneg'] = str(long(0))
      btc_balance['propertyinfo'] = getpropertyraw(btc_balance['id'])
      balance_data['balance'].append(btc_balance)
    #print "<<< end get_balancedata",balance_data
    return balance_data

def get_balancedata(address):
    print ">>> enter into get_balancedata",address
    addr = re.sub(r'\W+', '', address) #check alphanumeric
    #ROWS = get_balancedata_db_ROWS(address)
    ROWS = get_balancedata_rpc_ROWS(address)
    print ">>>>>>>>>>>>>>>>>>>>>>"
    print ROWS
    balance_data = { 'balance': [] }
    ret = bc_getbalance(addr)
    out = ret['bal']
    err = ret['error']
    for balrow in ROWS:
        cID = str(int(balrow['propertyid'])) #currency id
        sym_t = ('BTC' if cID == '0' else ('OMNI' if cID == '1' else ('T-OMNI' if cID == '2' else 'SP' + cID) ) ) #symbol template
        #1 = new indivisible property, 2=new divisible property (per spec)
        #divi = True if int(balrow[1]) == 2 else False
        divi =  True
        res = { 'symbol' : sym_t, 'divisible' : divi, 'id' : cID }
        #inject property details but remove issuanecs
        res['propertyinfo'] = getpropertyraw(cID)
        if 'issuances' in res['propertyinfo']:
          res['propertyinfo'].pop('issuances')
        #res['pendingpos'] = str(long(balrow[3]))
        #res['pendingneg'] = str(long(balrow[4]))
        res['reserved'] = str(float(balrow['reserved'])*100000000)
        res['frozen'] = str(balrow['frozen'])
        if cID == '0':
          print "if"
          #get btc balance from bc api's
          if err != None or out == '':
            #btc_balance[ 'value' ] = str(long(-555))
            btc_balance[ 'value' ] = str(long(0))
            btc_balance[ 'error' ] = True
          else:
            try:
              if balrow[4] < 0:
                #res['value'] = str(long( json.loads( out )[0][ 'paid' ]) + str(long(balrow[4]))
                #res['value'] = str(long( json.loads( out )['data']['balance']*1e8) + str(long(balrow[4]))
                res['value'] = str(long( out ) + long(balrow[4]))
              else:
                #res['value'] = str(long( json.loads( out )[0][ 'paid' ]))
                #res['value'] = str(long( json.loads( out )['data']['balance']*1e8))
                res['value'] = str(long( out ))
            except ValueError:
              #btc_balance[ 'value' ] = str(long(-555))
              btc_balance[ 'value' ] = str(long(0))
              btc_balance[ 'error' ] = True
        else:
          if not balrow['frozen'] > 0:
            #update the 'available' balance immediately when the sender sent something. prevent double spend as long as its not frozen
            res['value'] = str(float(balrow['balance'])*100000000)
          else:
            res['value'] = str(float(balrow['balance'])*100000000)

        #res['reserved_balance'] = ('%.8f' % float(balrow[5])).rstrip('0').rstrip('.')
        balance_data['balance'].append(res)

    #check if we got BTC data from DB, if not trigger manually add
    addbtc=True
    for x in balance_data['balance']:
      if "BTC" in x['symbol']:
        addbtc=False

    if addbtc:
      btc_balance = { 'symbol': 'BTC', 'divisible': True, 'id' : '0', 'error' : False }
      if err != None or out == '':
        #btc_balance[ 'value' ] = str(long(-555))
        btc_balance[ 'value' ] = str(long(0))
        btc_balance[ 'error' ] = True
      else:
        try:
          #btc_balance[ 'value' ] = str(long( json.loads( out )[0][ 'paid' ]))
          #btc_balance[ 'value' ] = str(long( json.loads( out )['data']['balance']*1e8 ))
          btc_balance[ 'value' ] = str(long( out ))
        except ValueError:
          #btc_balance[ 'value' ] = str(long(-555))
          btc_balance[ 'value' ] = str(long(0))
          btc_balance[ 'error' ] = True
      btc_balance['pendingpos'] = str(long(0))
      btc_balance['pendingneg'] = str(long(0))
      btc_balance['propertyinfo'] = getpropertyraw(btc_balance['id'])
      balance_data['balance'].append(btc_balance)
    #print "<<< end get_balancedata",balance_data
    return balance_data


def get_bulkbalancedata(addresses):
    btclist=bc_getbulkbalance(addresses)

    retval = {}

    for address in addresses:
      addr = re.sub(r'\W+', '', address) #check alphanumeric
      ROWS=dbSelect("""select
                       f1.propertyid, sp.propertytype, f1.balanceavailable, f1.pendingpos, f1.pendingneg, f1.balancereserved, f1.balancefrozen
                     from
                       (select
                          COALESCE(s1.propertyid,s2.propertyid) as propertyid, COALESCE(s1.balanceavailable,0) as balanceavailable, COALESCE(s1.balancefrozen,0) as balancefrozen,
                          COALESCE(s2.pendingpos,0) as pendingpos,COALESCE(s2.pendingneg,0) as pendingneg, COALESCE(s1.balancereserved,0) as balancereserved
                        from
                          (select propertyid,balanceavailable,balancereserved,balancefrozen
                           from addressbalances
                           where address=%s) s1
                        full join
                          (SELECT atx.propertyid,
                             sum(CASE WHEN atx.balanceavailablecreditdebit > 0 THEN atx.balanceavailablecreditdebit ELSE 0 END) AS pendingpos,
                             sum(CASE WHEN atx.balanceavailablecreditdebit < 0 THEN atx.balanceavailablecreditdebit ELSE 0 END) AS pendingneg
                           from
                             addressesintxs atx, transactions tx
                           where
                             atx.txdbserialnum=tx.txdbserialnum
                             and tx.txstate='pending'
                             and tx.txdbserialnum<-1
                             and atx.address=%s
                           group by
                             atx.propertyid) s2
                        on s1.propertyid=s2.propertyid) f1
                     inner join smartproperties sp
                     on f1.propertyid=sp.propertyid and (sp.protocol='Omni' or sp.protocol='Bitcoin')
                     order by f1.propertyid""",(addr,addr))

      balance_data = { 'balance': [] }
      try:
        if address in btclist:
          out = btclist[address]
          err = None
        else:
          out = ''
          err = "Missing"
      except TypeError:
        out = ''
        err = "Missing"

      for balrow in ROWS:
        cID = str(int(balrow[0])) #currency id
        sym_t = ('BTC' if cID == '0' else ('OMNI' if cID == '1' else ('T-OMNI' if cID == '2' else 'SP' + cID) ) ) #symbol template
        #1 = new indivisible property, 2=new divisible property (per spec)
        divi = True if int(balrow[1]) == 2 else False
        res = { 'symbol' : sym_t, 'divisible' : divi, 'id' : cID }
        #inject property details but remove issuanecs
        res['propertyinfo'] = getpropertyraw(cID)
        if 'issuances' in res['propertyinfo']:
          res['propertyinfo'].pop('issuances')
        res['pendingpos'] = str(long(balrow[3]))
        res['pendingneg'] = str(long(balrow[4]))
        res['reserved'] = str(long(balrow[5]))
        res['frozen'] = str(long(balrow[6]))
        if cID == '0':
          #get btc balance from bc api's
          if err != None or out == '':
            #btc_balance[ 'value' ] = str(long(-555))
            btc_balance[ 'value' ] = str(long(0))
            btc_balance[ 'error' ] = True
          else:
            try:
              if balrow[4] < 0:
                #res['value'] = str(long( json.loads( out )[0][ 'paid' ]) + str(long(balrow[4]))
                res['value'] = str(long( out ) + long(balrow[4]))
              else:
                #res['value'] = str(long( json.loads( out )[0][ 'paid' ]))
                res['value'] = str(long( out ))
            except ValueError:
              #btc_balance[ 'value' ] = str(long(-555))
              btc_balance[ 'value' ] = str(long(0))
              btc_balance[ 'error' ] = True
        else:
          #get regular balance from db
          if balrow[4] < 0 and not balrow[6] > 0:
            #update the 'available' balance immediately when the sender sent something. prevent double spend
            res['value'] = str(long(balrow[2]+balrow[4]))
          else:
            res['value'] = str(long(balrow[2]))

        #res['reserved_balance'] = ('%.8f' % float(balrow[5])).rstrip('0').rstrip('.')
        balance_data['balance'].append(res)

      #check if we got BTC data from DB, if not trigger manually add
      addbtc=True
      for x in balance_data['balance']:
        if "BTC" in x['symbol']:
          addbtc=False

      if addbtc:
        btc_balance = { 'symbol': 'BTC', 'divisible': True, 'id' : '0' ,'error' : False}
        if err != None or out == '':
          #btc_balance[ 'value' ] = str(long(-555))
          btc_balance[ 'value' ] = str(long(0))
          btc_balance[ 'error' ] = True
        else:
          try:
            #btc_balance[ 'value' ] = str(long( json.loads( out )[0][ 'paid' ]))
            btc_balance[ 'value' ] = str(long(  out ))
          except ValueError:
            #btc_balance[ 'value' ] = str(long(-555))
            btc_balance[ 'value' ] = str(long(0))
            btc_balance[ 'error' ] = True
        btc_balance['pendingpos'] = str(long(0))
        btc_balance['pendingneg'] = str(long(0))
        btc_balance['propertyinfo'] = getpropertyraw(btc_balance['id'])
        balance_data['balance'].append(btc_balance)

      retval[address]=balance_data
    return retval
