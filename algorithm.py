import newtons
import numpy as np


# In[2]:

def initialW(k,X):
    W=[]
    for i in xrange(k):
        w=0.01*np.matrix([np.random.rand(X[0].shape[1])])
        W.append(w)
    return W


# In[3]:

def rho_i(W,X,Z,i):
    #z=1 ->failure
    rho=[]
    for t in xrange(len(X)):
        if Z[t]==1:
            rho.append(1)
        else:
            pro=1
            s=1
            for j in xrange(len(W)):
                temp=newtons.sigmoid_(X[t],W[j])
                if j==i:
                    s=temp
                pro=pro*temp
            rho.append((1.0*(s-pro))/(1-pro))
    return np.matrix(rho).T


# In[4]:

def log_likelihood(W,X,Z):
    s=0
    for t in xrange(len(X)):
        if Z[t]==1:
            m=0
            for j in xrange(len(W)):
                temp=np.log(newtons.sigmoid_(X[t],W[j]))
                m=m+temp
            s=s+m
        else:
            pro=1
            for j in xrange(len(W)):
                temp=newtons.sigmoid_(X[t],W[j])
                pro=pro*temp
            s=s+np.log(1-pro)
    return s


# In[ ]:

#algorithm
X,Z,X_test,Z_test=newtons.initialize()
k=2
W=initialW(k,X)
e=100
l=log_likelihood(W,X,Z)
while e>0.1:
    W1=W
    for i in xrange(k):
        rho=rho_i(W,X,Z,i)
        W1[i]=newtons.lr_newton(X,rho,W[i])
    W=W1
    l_new=log_likelihood(W,X,Z)
    e=l_new-l
    print "e ",e
    print "l ",l_new
    l=l_new


# In[6]:

z=[]
for t in xrange(len(X_test)):
    pro=1
    for j in xrange(len(W)):
        temp=newtons.sigmoid_(X_test[t],W[j])
        pro=pro*temp
    if pro>=0.5:
        z.append(1)
    else:
        z.append(0)


# In[8]:

count=0
for z1 in z:
    if z1==1:
        count+=1
print count,len(X_test)
z=np.matrix(z).T
print np.sum(np.subtract(Z_test,z))
