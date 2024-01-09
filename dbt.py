from sqlalchemy import text
from sqlmodel import select


cte = select(text('select * from foo'[7:])).cte('foo_cte')    
mnt = select(text("foo_cte.*, 'x' as x ")).select_from(cte).cte('foo_mnt') 
q = select('*').select_from(mnt)  


cte = select(text('select * from foo'[7:])).cte('foo_cte')   

'select * from foo'[7:]


cte = select(text('with ax as (select * from foo)')).cte('foo_cte')    


    




select_s = 'select * from foo'[7:]
x= select(select_s)
rescte = cstr(x)[7:]).cte('rescte')

