package org.openremote.manager.mbean;


import javax.management.MBeanServer;
import java.lang.management.ManagementFactory;

public class MBeanServerProducer {


    public MBeanServer mBeanServer() {
        return ManagementFactory.getPlatformMBeanServer();
    }

}
