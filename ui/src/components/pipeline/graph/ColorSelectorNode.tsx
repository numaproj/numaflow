import {Handle, NodeProps} from 'react-flow-renderer';
import {FC, memo} from "react";

const ColorSelectorNode: FC<NodeProps> = ({ data, isConnectable }) => {
    return (
        <>
            <Handle
                type="source"
                onConnect={(params) => console.log('handle onConnect', params)}
                isConnectable={isConnectable}
            />
            <div>
                Custom Color Picker Node: <strong>{data.color}</strong>
            </div>
            {/*<input*/}
            {/*    className="nodrag"*/}
            {/*    type="color"*/}
            {/*    onChange={data.onChange}*/}
            {/*    defaultValue={data.color}*/}
            {/*/>*/}
            {/*<Handle*/}
            {/*    type="source"*/}
            {/*    position="right"*/}
            {/*    id="a"*/}
            {/*    style={{ top: 10, background: '#555' }}*/}
            {/*    isConnectable={isConnectable}*/}
            {/*/>*/}
            {/*<Handle*/}
            {/*    type="source"*/}
            {/*    position="right"*/}
            {/*    id="b"*/}
            {/*    style={{ bottom: 10, top: 'auto', background: '#555' }}*/}
            {/*    isConnectable={isConnectable}*/}
            {/*/>*/}
        </>
    );
};
export default memo(ColorSelectorNode);

