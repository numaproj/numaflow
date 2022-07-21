import {memo} from 'react';

import {Handle} from 'react-flow-renderer';

const CustomNode = ({data, isConnectable}) => {
    return (
        <>
            <Handle
                type="target"
                position="left"
                style={{ background: '#555' }}
                onConnect={(params) => console.log('handle onConnect', params)}
                isConnectable={isConnectable}
            />
            <div>
                <div style={{width: "40px"}}>
                    <svg viewBox="0 0 10 10" xmlns="http://www.w3.org/2000/svg">
                        <circle cx="5" cy="5" r="5"/>
                    </svg>
                </div>
            </div>
            {/*<input*/}
            {/*    className="nodrag"*/}
            {/*    type="color"*/}
            {/*    onChange={data.onChange}*/}
            {/*    defaultValue={data.color}*/}
            {/*/>*/}
            <Handle
                type="source"
                position="bottom"
                id="2"
                style={{bottom: 0, background: '#555'}}
                isConnectable={isConnectable}
            />
            <Handle
                type="source"
                position="bottom"
                id="3"
                style={{bottom: 0, top: 'auto', background: '#555'}}
                isConnectable={isConnectable}
            />
        </>
    );
}

export default memo(CustomNode);