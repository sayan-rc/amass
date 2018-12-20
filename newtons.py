
#Logistic Regression with newtons method
import math
import numpy as np
import random


# In[3]:

def preprocess():
    #preprocessing
    y_train=[]
    with open('Ytrain.txt') as f:
        y_train=map(int,f.readlines())

    y_test=[]
    with open('Ytest.txt') as f:
        y_test=map(int,f.readlines())

    x_train=[]
    b=open('Xtrain.txt')
    for line in b.readlines():
        l=line.strip().strip(']').strip('[').split(',')
        if l[-1].strip()=='True':
            l[-1]='1'
        else:
            #print l[-1]
            l[-1]='0'
        x_train.append(map(float,l))

    x_test=[]
    b=open('Xtest.txt')
    for line in b.readlines():
        l=line.strip().strip(']').strip('[').split(',')
        if l[-1].strip()=='True':
            l[-1]='1'
        else:
            #print l[-1]
            l[-1]='0'
        x_test.append(map(float,l))

    return x_train,y_train,x_test,y_test


# y=[]
# for y_ in y_train[100:500]:
#     if y_==0:
#         y.append(0.25)
#     else:
#         y.append(0.75)

# len(x_train), y_train[415]

# In[44]:

def initialize():
    x,y,xt,yt=preprocess()
    for x1 in x:
        x1.append(1.0)
    for x1 in xt:
        x1.append(1.0)
    for i in xrange(len(y)):
        if y[i]==1:
            y[i]=0
        else:
            y[i]=1
    for i in xrange(len(yt)):
        if yt[i]==1:
            yt[i]=0
        else:
            yt[i]=1
    X1=np.matrix(x)
    Y1=np.matrix(y)
    Xt=np.matrix(xt)
    Yt=np.matrix(yt)
    return X1, Y1.T, Xt, Yt.T


# In[35]:

def sigmoid_(xi,w):
    #print np.dot(xi,w)
    #xi.reshape(xi.shape[0],1)
    #print np.dot(xi,w.T)[0][0]
    xi=xi.A1
    #print xi
    w=w.A1
    z=np.dot(xi,w)
    return 1/(1+np.exp(-(z)))


# In[36]:

def log_likelihood(y,sigmoids):
    l=0
    sigmoids=sigmoids.A1
    y=y.A1
    for i in xrange(len(y)):
        l+=(y[i]*np.log(sigmoids[i]))+((1-y[i])*np.log(1-sigmoids[i]))
    return l


# In[37]:

def gradient(x,y,sigmoids):
    #print x.T.shape,(y-sigmoids).shape
    o=np.matrix(np.ones(len(sigmoids))).T
    #print o.shape,sigmoids.shape,y.shape
    g=x.T*((sigmoids)-y)
    return g


# In[38]:

def invhessian(x,sigmoids):
    os=(np.matrix(np.ones(len(sigmoids))).T - sigmoids).A1
    #print os
    s=sigmoids.A1
    #print s
    sigs=[]
    for i in xrange(len(s)):
        sigs.append(s[i]*os[i])
    d=np.diag(sigs)
    #print d
    #print np.shape(x.T),np.shape(d)
    q=x.T*np.matrix(d)
    p=q*np.matrix(x)
    #print np.shape(p)
    return np.linalg.pinv(p)


# In[39]:

def get_sigmoids(x,w):
    sig=[]
    for i in xrange(len(x)):
        s=sigmoid_(x[i],w)
        #print s
        sig.append(s)
    sig=np.array(sig)
    return np.matrix(sig).T


# In[71]:

def lr_newton(X1,Y1,W):
    a=1.0
    e=100
    X=X1#np.matrix(X1)
    #print X1.shape
    w=W
    Y=Y1 #np.matrix(Y1.T)
    #print Y.shape
    #n=0.01
    #w=n*(np.matrix([np.random.rand(X[0].shape[1])]))
    #print w.shape
    #print w[0]
    #print len(X[0]),len(w)
    while(1):
        sigmoids=get_sigmoids(X,w)
        #print sigmoids.shape
        l=log_likelihood(Y,sigmoids)
        #print l
        while e>0.1:
            v=np.matrix(invhessian(X,sigmoids))
            #print v.shape
            u=np.matrix(gradient(X,Y,sigmoids))
            #print u.shape
            z=(v*u)
            #print z.shape,w.shape
            w1=w-(a*z.T)
            #print w.shape
            w=w1
            sigmoids=get_sigmoids(X,w)
            #print sigmoids.shape
            l1=log_likelihood(Y,sigmoids)
            #print l,l1
            e=l1-l
            #print e
            l=l1
        if e<=0.1 and e>=0:
            break
        w=W
        a=a/2
        print a
        e=100
    return w


# In[61]:

def mae(X1,Y1,w):
    y1=[1/(1+np.exp(-(np.dot(x.A1,w.A1)))) for x in X1]
    y1=np.matrix(y1)
    return (1.0/len(X1))*np.sum(np.subtract(Y1,y1))


# In[ ]:




