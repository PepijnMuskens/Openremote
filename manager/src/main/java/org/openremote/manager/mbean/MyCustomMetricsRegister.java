package org.openremote.manager.mbean;

import javax.annotation.PreDestroy;
import javax.management.*;


public class MyCustomMetricsRegister {


    private MBeanServer mBeanServer;


    private CustomMetricsMBean myCustomMetricsMBean;

    private ObjectName name;

    void init(Object event)
            throws NotCompliantMBeanException,
            InstanceAlreadyExistsException,
            MBeanRegistrationException,
            MalformedObjectNameException {

        final Class<? extends CustomMetricsMBean> objectClass = myCustomMetricsMBean.getClass();

        name = new ObjectName(
                String.format("%s:type=basic,name=%s", objectClass.getPackage().getName(), objectClass.getName())
        );

        mBeanServer.registerMBean(myCustomMetricsMBean, name);
    }

    @PreDestroy
    void preDestroy() {
        try {
            mBeanServer.unregisterMBean(name);
        } catch (InstanceNotFoundException | MBeanRegistrationException e) {
            throw new RuntimeException(e);
        }
    }


}
