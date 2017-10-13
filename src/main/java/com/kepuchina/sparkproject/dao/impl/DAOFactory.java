package com.kepuchina.sparkproject.dao.impl;

import com.kepuchina.sparkproject.dao.ITaskDao;

/**
 * Created by Zouyy on 2017/9/28.
 */
public class DAOFactory {

    /**
     * 任务管理DAO
     * @return
     */
    public static ITaskDao getTaskDao(){
        return new TaskDaoImpl();
    }
}
