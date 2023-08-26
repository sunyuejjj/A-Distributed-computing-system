package com.ksc.wordcount.thrift.service;

import org.apache.thrift.TException;

import java.util.List;

public class UrlTopNServiceiml implements UrlTopNService.Iface{
    @Override
    public UrlTopNAppResponse submitApp(UrlTopNAppRequest urlTopNAppRequest) throws TException {

        return null;
    }

    @Override
    public UrlTopNAppResponse getAppStatus(String applicationId) throws TException {
        return null;
    }

    @Override
    public List<UrlTopNResult> getTopNAppResult(String applicationId) throws TException {
        return null;
    }
}
