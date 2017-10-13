package com.kepuchina.sparkproject.dao;

import com.kepuchina.sparkproject.domain.Task;

/**
 * Created by Zouyy on 2017/9/28.
 */
public interface ITaskDao {

    /**
     * 根据主键查询任务
     * @param taskid
     * @return
     */
    public Task findById(long taskid);
}
