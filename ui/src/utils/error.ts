import { toast } from 'react-toastify';

export const notifyError = (
  errorList: any[]
) => {

  if (errorList) {
    errorList.forEach((error: any, idx: number) => {
      toast.error(
        error.error,
        {...error.options, delay: idx * 1000}
      );
    })
  }

}
