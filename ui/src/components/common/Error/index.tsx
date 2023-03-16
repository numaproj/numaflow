import { toast, ToastContainer } from 'react-toastify';
import "react-toastify/dist/ReactToastify.css";
import {useEffect} from "react";

interface ErrorContentProps {
  error: string;
}

export function ErrorContent(props: ErrorContentProps) {
  const { error } = props;

  useEffect(() => {
    if (error.length) {
      toast.error(error);
    }
  }, [error]);

  return (
    <div>
      <ToastContainer
        position="bottom-right"
        autoClose={6000}
        hideProgressBar={false}
        newestOnTop={false}
        closeOnClick={false}
        rtl={false}
        draggable={true}
        pauseOnHover={true}
        theme="light"
      />
    </div>
  );
}
