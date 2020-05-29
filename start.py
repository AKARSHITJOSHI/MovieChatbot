from flask import Flask, render_template, request, redirect, url_for,session,abort
import os
import pandas as pd
import re
import csv
import mysql.connector
from shutil import copyfile
import nltk
from nltk.corpus import stopwords 
from nltk.tokenize import word_tokenize 

app = Flask(__name__)
app.config['TEMPLATES_AUTO_RELOAD'] = True
pd.set_option('max_colwidth', 100)
# Make the WSGI interface available at the top level so wfastcgi can get it.
wsgi_app = app.wsgi_app
df = pd.DataFrame()
admin_password = "admin"
admin_email = "admin@chatbot.com"
app.secret_key = os.urandom(12)


def initialize():
    global df
    df = pd.read_csv('movie.csv', error_bad_lines=False, encoding="utf-8")
    df.dropna()
    df.columns = [w.lower() for w in list(df.columns)]
    for w in list(df.columns):
        df[w] = df[w].astype('str')
    df = df.apply(lambda x: x.astype(str).str.lower())
    #cleaning for non utf-8
    u = df.select_dtypes(object)
    df[u.columns] = u.apply(
        lambda x: x.str.encode('ascii', 'ignore').str.decode('ascii'))
    os.remove("templates/chat.html")
    copyfile("templates/chatTemplate1.html", "templates/chat.html")


def addMovie(row):
    global df
    sf=df.append(row)
    changeDf(sf)
def results(h,ab,cardinality=15):
    ah = h.loc[:,ab].head(cardinality)
    st = "<table><tr>"
    for a in ab:
        st = st+"<th>"+a+"</th>"
    st = st+"</tr>"
    for row in ah.itertuples(index=True, name='Pandas'):
        st = st+"<tr>"
        for a in ab:
            st = st+"<td>"+getattr(row, a)+"</td>"
        st = st+"</tr>"
    st = st+"</table>"
    return st


def exactKeysearch(content, colname):

    regexstr = "(.*\s+)?"+content+"(:.*|\s+.*|,.*|-.*)?$"
    s = df[df.loc[:, colname].str.match(pat=regexstr)]
    return s



def search(content, colname):
    regexstr = "^"+content+"$"
    s = df[df.loc[:, colname].str.match(pat=regexstr)]
    return s


def particularColKeysearch(content, colname):

    regexstr = ".*"+content+".*"
    s = df[df.loc[:, colname].str.match(pat=regexstr)]
    return s


def multikeysearch(content, colname):
    a = ""
    for c in content:
        if c:
            a = a+"(?=.*\\b"+c+")"
    regexstr = a+".*"
    
   
    s = df[df.loc[:, colname].str.match(pat=regexstr)]
    if not s.empty:
        return s
    else:
        a = ""
        for c in content:
            if c:
                a = a+"(?=.*"+c+")"
        regexstr = a+".*"

        s = df[df.loc[:, colname].str.match(pat=regexstr)]
        return s
def fun(value):
    return value.size
def finalsearch(query):
    
    stop_words = set(stopwords.words('english')) 
    word_tokens = query.split(" ")
     
    query= [] 
    
    for w in word_tokens: 
        if w not in stop_words: 
            query.append(w) 
    Genre=['action','drama','romance','thriller','horror','biography','crime','comedy','family','adventure']   
    querycols=[]
    lst2=[]
    lst=[]
    querycols=[w for w in list(df.columns) if w in query]
    if "movie" not in querycols:
        querycols.append("movie")

    querygenre=[w for w in query if w in Genre]
    keywords=[w for w in query if w not in querycols]
    for w in keywords:
        if w.isdigit():
            if len(w)>2: querycols.append('year')
            else: 
                querycols.append('rating')
            break
    if len(querycols)==1 and "movie" in querycols:
        querycols.append("cast")
    for w in keywords:
            for c in querycols:
                w="(?=.*\\b"+w+"\\b)"
                lst.append(df[df[c].str.contains(w)])
                
    
    for  s in lst:
        if s.size>0:
            lst2.append(s)
    lst2.sort(key=fun,reverse=True)
    i=0
    while i<len(lst2)-1:
        
        isection = pd.merge(lst2[i], lst2[i+1], how='inner')
        
        if isection.empty: 
            print("hola")
            
            i=i+1
        
        else:
            del lst2[i+1]
            lst2[i]=isection
            
    finaldf=pd.DataFrame()
    for l in lst2:
        finaldf=finaldf.append(l)
        
        
    return finaldf
                
        
  


def changeDf(df):
    df.to_csv('./sample.csv', header=True, index=False)
    os.remove("./movie.csv")
    os.rename("./sample.csv", "./movie.csv")
    initialize()

def updateIndex(choice,value,h):
    if choice == "Genre":
        df.at[h, "genre"] = value
    elif choice == "Rating":
        df.at[h, "rating"] = value
    elif choice == "Revenue":
        df.at[h, "revenue"] = value
    elif choice == "Runtime":
        df.at[h, "runtime"] = value
    elif choice == "Year":
        df.at[h, "year"] = value
    elif choice == "Cast":
        df.at[h, "cast"] = value
    changeDf(df)
    return "Update succesfull"

def sameMovieName(h,colname):
    
    if len(h)==1:
        colname="".join(colname)
        st=h[colname].to_string(index=False)
        return st
    else:
        if len(h.year.unique())>1:
            colname.insert(1, 'year')
        colname.insert(0, 'movie')
        
        colname = list(set(colname))
        return results(h,colname)


def process(query):
    retmsg=[]
    query = query.lower()
    query = re.sub("\?"," ", query)
    query =query.strip('.')
    query=re.sub(" +"," ",query)
    query = re.sub(" *,+ *", " ", query)
    if query.isspace():
        return "Please Specify Query"
    f1=1
    f = 1
    Genre=['action','drama','romance','thriller','horror','biography','crime','comedy','family','adventure']
    s=[q for q in Genre if q in query]
    if len(s)>0:
        f1=1
    else:
        f1=0
    ab = [q for q in list(df.columns) if q in query]
    if "movie" not in ab:
        ab.insert(0,'movie')
    ab=list(set(ab))
    if len(ab)==2:
        f=1
    else:
        f=0
   
    m = re.match(
        r'^.*?(movies?\s)?(having\s|with\s|of\s)?rating\s(of)?(.*?)(movies?.*)?$', query)
    if m:
        a=""
        if not m.group(4):
            retmsg.append( "Please specify rating:range or moviename whose rating is to be found")
        
        elif (not m.group(1) and (not m.group(5))):
            retmsg.append("Incomplete query")



        else:
            
            t = re.match(r'^(\d)(\.\d)?$',m.group(4).strip(' '))
            if t:
                 
                 if  t.group(2)==None:
                     a=t.group(1)+".0"
                 else:
                     a=t.group(1)+t.group(2).strip(' ')

                 h = search(a, "rating")
                 if not h.empty:
                     st = h["movie"].head(20).to_string(index=False)
                     return(st)
                 else:
                     retmsg.append("movies having : "+a+" rating not found")
            else:
                retmsg.append("No such rating exist pls specify rating in range of 1-10.")
    
    m = re.match(
        r'.*\s*(actors?|actress)\sof\s(movies?\s)?(.*)', query)
    if m:
        h = search(m.group(3).strip(' '), "movie")
        if not h.empty:
            st = h[['movie','cast']].to_string(index=False)
            return st
        else:
            h = exactKeysearch(m.group(3).strip(' '), "movie")
            if not h.empty:
                st = results(h,['movie','cast'])
                return st
            else:
                content = m.group(3).strip(' ')
                content = content.split(' ')
                h = multikeysearch(content, "movie")
                if not h.empty:
                    st = results(h, ['movie','cast'])
                    return st
                else:
                    retmsg.append("moviename:"+m.group(3)+" not found")
    
    
    m = re.match(
        r'^(what|find)?.*\s*(cast|revenue|director|runtime|genre|rating|year)\s*(of|in)\s(?=(the\s)?)(movie\s|film\s)?(.*)$', query)
    if m and f:
         if not m.group(6):
            retmsg.append("Please specify some more information")
         
         h = search(m.group(6).strip(' '), "movie")
         if not h.empty:
             st=sameMovieName(h,[m.group(2)])
             
             return st
         else:
             h = exactKeysearch(m.group(6).strip(' '), "movie")
             if not h.empty:
                 st = results(h,ab)
                 return st
             else:
                
                content = m.group(6).strip(' ')
                content = content.split(' ')
                h = multikeysearch(content, "movie")
                if not h.empty:
                    st = results(h,['movie',m.group(2)])
                    return st
                else:
                    retmsg.insert(0,"moviename:"+m.group(6)+" not found")
    
    
    m = re.match(
        r'^(show|find movie|find|list|movie)?(.*?)(\smovies?\s)?\s*(cast|revenue|director|runtime|genre|rating|year)$', query)
    if m and (not f1):
        if not m.group(2):
            retmsg.append("Please specify some more information")
        h = search(m.group(2).strip(' '), "movie")
        if not h.empty:
            st=sameMovieName(h,[m.group(4)])
            return st
        else:
            h = exactKeysearch(m.group(2).strip(' '), "movie")
            if not h.empty:
                 st = results(h, ab)
                 return(st)
            else:
                content=m.group(2).strip(' ')
                content=content.split(' ')
                h=multikeysearch(content,"movie")
                if not h.empty:
                    st=results(h,['movie',m.group(4)])
                    return st
                else:
                    retmsg.append("moviename: "+m.group(2)+" not found")
    
    
    m = re.match(r'^(find|list|show)?\s*movies?(\sof|\sof year|\sreleased in|\sreleased in year)\s(\d{1,4})$', query)
    if m:
        if not m.group(3):
            retmsg.append("Please specify some more information")
        h = search(m.group(3).strip(' '), "year")
        if not h.empty:
            st = sameMovieName(h, ['movie'])
            return st
        else:
            retmsg.append("year: "+m.group(3)+" not found")
    
   

    m = re.match( r'^.*?\s*movies?(\sof actor|\sof actress|\sof director|\sdirected by|\sof|\sby)\s(.*)$', query)
    if m:
        if not m.group(1):
            retmsg.append("Please specify name to procede")
        if "actor" in m.group(1) or "actress" in m.group(1) or "of" == m.group(1).strip(' '):
            h = exactKeysearch(m.group(2).strip(' '), "cast")
            if not h.empty:
                st = h["movie"].head(9).to_string(index=False)
                return st
            else:
                 retmsg.append("Actor: "+m.group(2)+" not found")
        elif "director" in m.group(1) or "directed" in m.group(1) or "by" in m.group(1):
            h = search(m.group(2).strip(' '), "director")
            if not h.empty:
                st = sameMovieName(h, ['movie'])
                return st
            else:
                h = exactKeysearch(m.group(2).strip(' '), "director")
                if not h.empty:
                    l1=['movie','director']
                    st = results(h,l1)
                    return st
                else:
                    retmsg.append("Director Name: "+m.group(2)+" not found")
    
    m = re.match(r'^.*?\s*((cast[\s,](and\s)?|revenue[\s,](and\s)?|rating[\s,](and\s)?|runtime[\s,](and\s)?|director[\s,](and\s)?|genre[\s,](and\s)?|year[\s,](and\s)?)+)(of movies?|of)(.*)$', query)
    if m and (not f):
        if not m.group(11):
            retmsg.append("Please specify movie name")
        else:
            st = ""
            r=[]
            a=m.group(1)
            b = re.split('and|,|\s', a)
            for c in b:
                if c:
                    r.append(c)
            r.insert(0,'movie')
            h = search(m.group(11).strip(' '), "movie")
            if not h.empty:
                if len(h)>1:
                    r.insert(1,'year')
                    r=list(set(r))
                st = results(h,r)
                return st
                
            else:
                h = exactKeysearch(m.group(11).strip(' '), "movie")
                if not h.empty:
                    if len(h) > 1:
                        r.insert(1, 'year')
                        r = list(set(r))
                    st = results(h,r)
                    return st
                else:
                    content = m.group(11).strip(' ')
                    content = content.split(' ')
                    h = multikeysearch(content, "movie")
                    if not h.empty:
                        st = results(h,r)
                        return st
                    else:
                        retmsg.append("moviename: "+m.group(11)+" not found")
    
        
    m = re.match(
        r'^.*?\s*((action[\s,](and\s)?|thriller[\s,](and\s)?|drama[\s,](and\s)?|comedy[\s,](and\s)?|romance[\s,](and\s)?|horror[\s,](and\s)?|biography[\s,](and\s)?)+)(movies?|genre(\smovies?)?)(\slist)?$', query)
    if m:
        a = m.group(1).strip(' ')
        b = re.split('and|,|\s', a)
        
        a = multikeysearch(b, "genre")
        if not a.empty:
            st=a['movie'].head(30).to_string(index=False)
            return st
        else:
            return "No movie with genre:"+" ".join(b)
    
    
    m = re.match(
        r'^(search|show|find movie|find|list|movie)?(.*?)(\smovies?.*|\sdetails)?$', query)
    if m:
        if not m.group(2):
            retmsg.append("Please specify moviename")
        else:
            r = ['movie', 'year', 'genre', 'cast',
             'rating', 'director', 'runtime', 'revenue']
            h = search(m.group(2).strip(' '), "movie")
            if not h.empty:
                st = results(h, r)
                return st
            else:
                h = exactKeysearch(m.group(2).strip(' '), "movie")
                if not h.empty:
                    st = results(h, r)
                    return st
                else:
                    content = m.group(2).strip(' ')
                    content = content.split(' ')
                    h = multikeysearch(content, "movie")
                    if not h.empty:
                        st = results(h, r)
                        return st
                    else:
                        retmsg.append("moviename: "+m.group(2)+" not found")


    m = re.match(r'.*\s*top\s(\d+)\smovies?\s*(list|of all time)?', query)
    if m:

        r = ['movie', 'year', 'genre', 'cast',
             'rating', 'director', 'runtime', 'revenue']
        
        
        df2 = df.sort_values(by=['rating'],ascending=False)
        st = results(df2, r, int(m.group(1).strip(' ')))
        return st
    
    m = re.match(r'.*\s*(movies?|title|movie title)\s*(.*)',query)
    if m:
        if not m.group(2):
            retmsg.append("Please specify moviename")
        else:
            r=['movie','year','genre','cast','rating','director','runtime','revenue']
            h = search(m.group(2).strip(' '), "movie")
            if not h.empty:
                st = results(h, r)
                return st
            else:
                h = exactKeysearch(m.group(2).strip(' '), "movie")
                if not h.empty:
                    st = results(h, r)
                    return st
                else:
                    content=m.group(2).strip(' ')
                    content=content.split(' ')
                    h=multikeysearch(content,"movie")
                    if not h.empty:
                        st=results(h,r)
                        return st
                    else:
                        retmsg.append("moviename: "+m.group(2)+" not found")
        
        

   
    if len(retmsg)>0:
        h=finalsearch(query)
        if not h.empty:
        

            st=results(h,ab)
            return st
        else:
            return retmsg[0]
    return "sorry i didnt get you :("


@app.route('/')
def start():

    return render_template("home.html", status="hidden")


@app.route('/chat')
def chat():
  return render_template("chat.html", status="visible")


@app.route('/admin', methods=['GET', 'POST'])
def admin():
    global df
    
    if request.method == 'POST' and 'admin' in session:
        fname = request.form['form']
        mname = request.form['mname'].lower()
        mname =mname.strip(' ')

        if fname == "1":
            year = request.form.get('year')
            year=year.strip(' ')
            mcast = request.form['mcast']
            rating = request.form['rating']
            dname = request.form['dname']
            mcast = request.form['mcast']

            runtime = request.form.get('runtime')
            revenue = request.form.get('revenue')
            genre = request.form.getlist('genre')
            h = search(mname, "movie")
             
            row=pd.DataFrame({"movie":[mname],
                                "year":[year],
                                "cast":[mcast],
                                "runtime":[runtime],
                                "rating":[rating],
                                "director":[dname],
                                "revenue":[revenue],
                                
                                "genre":[",".join(genre)]
                             }
                            )
            if h.empty:
                addMovie(row)
                return render_template("admin.html", sc="success", val="f1")
            elif len(h) >= 1:
                 if not year:
                     return render_template("admin.html", sc="failed (movie exists) specify year", val="f1")
                 else:
                     h = df[df["movie"] == mname]
                     h=h[h['year']==year].index.values
                     if len(h)==0:
                         addMovie(row)
                         return render_template("admin.html", sc="success", val="f1")
                   
                     else:
                         return render_template("admin.html", sc="failed (moviename and year) exists", val="f1")
                    

        elif fname == "3":
            year = request.form.get('year')
            if not year:
                h = df[df["movie"] == mname]
                if len(h) == 1:

                    df = df[df["movie"] != mname]
                    changeDf(df)
                    return render_template("admin.html", sc="success", val="f3")
                elif len(h)>1:
                    return render_template("admin.html", sc="failed "+str(len(h))+" Entries of "+ mname+" found please specify year of release", val="f3")
                else:
                    return render_template("admin.html", sc="failed no results found ", val="f3")

            else:
                h = df[df["movie"] == mname]
                h=h[h['year']==year].index.values
                if len(h) == 1:
                    df=df.drop(h)
                    changeDf(df)
                    return render_template("admin.html", sc="success", val="f3")
                else:
                    return render_template("admin.html", sc="error", val="f3")
        elif fname == "2":
            choice = request.form.get("s2")
            value = request.form["mfy"]
            year=request.form['year']
            h = search(mname, "movie").index.values
            if len(h) == 1:
                rs=updateIndex(choice,value,h)
                return render_template("admin.html", sc=rs, val="f2")

            elif len(h) == 0:
                return render_template("admin.html", sc="update failed "+mname+" doesnot exist", val="f2")
            elif len(h)>1:
                if not year:
                     return render_template("admin.html", sc="failed "+str(len(h))+" Entries found specify year to proceed", val="f2")
                else:
                    h = df[df["movie"] == mname]
                    h=h[h['year']==year].index.values
                    if len(h)==0:
                        return render_template("admin.html", sc="Wrong entry for release year of moviename: "+mname, val="f2")
                   
                    elif len(h==1):
                        rs=updateIndex(choice,value,h)
                        return render_template("admin.html", sc=rs, val="f2")


    if 'admin' in session:
        return render_template("admin.html")
    else:
        return "Eror 404 page not found"


@app.route('/user/chat')

def userChat():
    if 'uname' in session:
        userchat = session['uname']
        return render_template(userchat+"/chat.html", sc="last")


@app.route('/user')
def user():
    if 'uname' not in session:
        return "eror 404"
    else:

        return render_template("user.html")


@app.route('/chatbox', methods=['POST'])
def chatbox():
    if 'uname' in session:
        uname = session['uname']

        if request.method == 'POST':
            query = request.form['query'].replace('\n', ' ')
            

            if not query.isspace() and query:
            
                bot_response = process(query).replace(
                    "\n", "<br>").replace(",", "<br>")
                flag = 0
                ftemp = open("templates/temp.html", "w")
                with open("templates/"+uname+"/chat.html", "r") as f:
                    for st in f:
                        if "<div id=\"Last\">" in st and "</div>" in st:
                            ftemp.write(st.replace(
                                '<div id=\"Last\">', '').replace('</div>', ''))
                            ftemp.write("<p class=\"qtext\">"+query+"</p>\n")
                            ftemp.write(
                                "<div id=\"Last\"><p class=\"reply\">"+bot_response+"</p>"+"</div>\n")
                        elif "<div id=\"Last\">" in st:
                            flag = 1
                            ftemp.write(st.replace('<div id=\"Last\">', ''))
                        elif flag:
                            if "</div>" in st:
                                flag = 0
                                ftemp.write(st.replace("</div>", ''))
                                ftemp.write(
                                    "<p class=\"qtext\">"+query+"</p>\n")
                                ftemp.write(
                                    "<div id=\"Last\"><p class=\"reply\">"+bot_response+"</p>"+"</div>\n")
                            else:
                                ftemp.write(st)
                        else:
                            ftemp.write(st)

                ftemp.close()
                f.close()
                os.remove("templates/"+uname+"/chat.html")
                os.rename("templates/temp.html",
                          "templates/"+uname+"/chat.html")
    return render_template(uname+"/chat.html",scroll="last")


@app.route('/trylogin')
def home():
    if not session.get('logged_in'):
        return render_template('login.html')
    else:
        return "Hello Boss!"

@app.route('/history')
def history():
    if 'uname' in session:
        uname=session['uname']
        os.remove("templates/"+uname+"/chat.html")
        copyfile("templates/chatTemplate.html", "templates/"+uname+"/chat.html")
    return render_template("user.html",msg="Deleted chat history")
@app.route('/logout')
def logout():
    if 'uname' in session:
        session.pop('uname',None)
    if 'admin' in session:
        session.pop('admin',None)
    return render_template("home.html", status="hidden")

    
    

@app.route('/login', methods=['POST'])
def do_check_login():
    db = mysql.connector.connect(host='localhost',
                                              database='login',
                                              user='root',
                                              password='password')
    cursor = db.cursor()
    check_email = request.form.get('email')
    check_pass = request.form.get('password')
    if check_email == admin_email and check_pass == admin_password:
        session['admin']=admin_email
        return render_template("admin.html", sc="welcome to Admin dashboard", val="f0")
    else:
        query = " SELECT username from test where password='%s' and email='%s' " % (
            check_pass, check_email)
        cursor.execute(query)
        data = cursor.fetchone()
        cursor.close()
        if data is None:
            return 'You need to signup'
        else:
            session['uname'] = check_email
            return redirect(url_for('user'))


@app.route('/signup')
def sign():
    return render_template('signup.html')


name = ''
email = ''
password = ''
@app.route('/signdetails', methods=['POST'])
def insert():
    db = mysql.connector.connect(host='localhost',
                                              database='login',
                                              user='root',
                                              password='password')
    cursor = db.cursor()
    name = request.form['username']
    email = request.form['email']
    password = request.form['psw']
    send = f'{name} has successfully signed up with email:{email} 👍'
    sql = "INSERT INTO `test` (`email`, `username`,`password`) VALUES (%s, %s,%s)"
    cursor.execute(sql, (f'{email}', f'{name}', f'{password}'))
    db.commit()
    db.close()
    os.mkdir("./templates/"+email)
    f = open("./templates/chatTemplate.html", "r")
    with open("./templates/"+email+"/chat.html", "w") as fp:
        for line in f:
            fp.write(line)
    f.close()
    fp.close()
    cursor.close()
    return '<h1> %s</h1>' % send


@app.route('/chat', methods=['POST'])
def fcall():

      if request.method == 'POST':
          query = request.form['query'].replace('\n', ' ')
        
          if not query.isspace() and query:

              bot_response = process(query).replace("\n", "<br>").replace(",", "<br>")
              flag = 0
              ftemp = open("templates/temp.html", "w")
              with open("templates/chat.html", "r") as f:
                  for st in f:
                      if "<div id=\"Last\">" in st and "</div>" in st:
                          ftemp.write(st.replace(
                              '<div id=\"Last\">', '').replace('</div>', ''))
                          ftemp.write("<p class=\"qtext\">"+query+"</p>\n")
                          ftemp.write(
                              "<div id=\"Last\"><p class=\"reply\">"+bot_response+"</p>"+"</div>\n")
                      elif "<div id=\"Last\">" in st:
                           flag = 1
                           ftemp.write(st.replace('<div id=\"Last\">', ''))
                      elif flag:
                          if "</div>" in st:
                              flag = 0
                              ftemp.write(st.replace("</div>", ''))
                              ftemp.write("<p class=\"qtext\">"+query+"</p>\n")
                              ftemp.write(
                                  "<div id=\"Last\"><p class=\"reply\">"+bot_response+"</p>"+"</div>\n")
                          else:
                              ftemp.write(st)
                      else:
                          ftemp.write(st)

              ftemp.close()
              f.close()
              os.remove("templates/chat.html")
              os.rename("templates/temp.html", "templates/chat.html")
      return render_template("chat.html", scroll="last")


@app.route('/about')
def about():
  return render_template("about.html")


if __name__ == '__main__':
    
    initialize()
    HOST = os.environ.get('SERVER_HOST', 'localhost')
    try:
        PORT = int(os.environ.get('SERVER_PORT', '7860'))
    except ValueError:
        PORT = 7860
    app.run(HOST, PORT, debug=True)
    
