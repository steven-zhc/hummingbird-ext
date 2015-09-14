package com.hczhang.hummingbird.ext.eventlog.kafka;

import com.hczhang.hummingbird.spring.ExtensionBinding;
import com.hczhang.hummingbird.spring.ExtensionType;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.ParserContext;
import org.w3c.dom.Element;

/**
 * Created by steven on 8/12/15.
 */
public class KafkaEventLogBinding implements ExtensionBinding {
    @Override
    public ExtensionType getExtensionType() {
        return ExtensionType.EVENT_LOG;
    }

    @Override
    public boolean lookingFor(String type) {
        if (StringUtils.equals("kafka", type)) {
            return true;
        }
        return false;
    }

    @Override
    public Class<?> getImplementClass() {
        return KafkaEventLog.class;
    }

    @Override
    public void moreConfig(BeanDefinitionBuilder componentFactory, Element element, ParserContext parserContext) {

    }
}
